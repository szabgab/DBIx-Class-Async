package DBIx::Class::Async::Schema;

use strict;
use warnings;
use utf8;

use Carp;
use Future;
use Try::Tiny;
use Scalar::Util 'blessed';
use DBIx::Class::Async;
use DBIx::Class::Async::Storage;
use DBIx::Class::Async::ResultSet;
use DBIx::Class::Async::Storage::DBI;
use Data::Dumper;

our $METRICS;
use constant ASYNC_TRACE => $ENV{ASYNC_TRACE} || 0;

sub await {
    my ($self, $future) = @_;

    my $loop = $self->{_async_db}->{_loop};
    my @results = $loop->await($future);

    # Unwrap nested Futures
    while (@results == 1
           && defined $results[0]
           && Scalar::Util::blessed($results[0])
           && $results[0]->isa('Future')) {

        if (!$results[0]->is_ready) {
            @results = $loop->await($results[0]);
        } elsif ($results[0]->is_failed) {
            my ($error) = $results[0]->failure;
            die $error;
        } else {
            @results = $results[0]->get;
        }
    }

    return wantarray ? @results : $results[0];
}

sub connect {
    my ($class, @args) = @_;

    # Separate async options from connect_info
    my $async_options = {};
    if (ref $args[-1] eq 'HASH' && !exists $args[-1]->{RaiseError}) {
        $async_options = pop @args;
    }

    my $schema_class = $async_options->{schema_class}
       or croak "schema_class is required in async options";

    my $schema_loaded = 0;
    if (eval { $schema_class->can('connect') }) {
        $schema_loaded = 1;
    }
    elsif (eval "require $schema_class") {
        $schema_loaded = 1;
    }
    elsif (eval "package main; \$${schema_class}::VERSION ||= '0.01'; 1") {
        $schema_loaded = 1;
    }

    unless ($schema_loaded) {
        croak "Cannot load schema class $schema_class: $@";
    }

    my $async_db = eval {
        DBIx::Class::Async->create_async_db(
            schema_class => $schema_class,
            connect_info => \@args,
            %$async_options,
        );
    };

    if ($@) {
        croak "Failed to create async engine: $@";
    }

    my $native_schema = $schema_class->connect(@args);

    my $self = bless {
        _async_db      => $async_db,
        _native_schema => $native_schema,
        _sources_cache => {},
    }, $class;

    # Populate the inflator map
    $async_db->{_custom_inflators} = $self->_build_inflator_map($native_schema);

    my $storage = DBIx::Class::Async::Storage::DBI->new(
        schema   => $self,
        async_db => $async_db,
    );

    $self->{_storage} = $storage;

    return $self;
}

# Cache specific
sub cache_hits    { shift->{_async_db}->{_stats}->{_cache_hits}   // 0 }
sub cache_misses  { shift->{_async_db}->{_stats}->{_cache_misses} // 0 }
sub cache_retries { shift->{_async_db}->{_stats}->{_retries}      // 0 }

# Execution specific
sub total_queries { shift->{_async_db}->{_stats}->{_queries}      // 0 }
sub error_count   { shift->{_async_db}->{_stats}->{_errors}       // 0 }
sub deadlock_count{ shift->{_async_db}->{_stats}->{_deadlocks}    // 0 }


sub class {
    my ($self, $source_name) = @_;

    croak("source_name required") unless defined $source_name;

    # Fetch metadata (this uses your existing _sources_cache)
    my $source = eval { $self->source($source_name) };

    if ($@ || !$source) {
        croak("No such source '$source_name'");
    }

    # Return the result class string (e.g., 'TestSchema::Result::User')
    return $source->{result_class};
}

sub clone {
    my $self = shift;
    my %args = @_;

    # 1. Determine worker count for the new pool
    my $worker_count = $args{workers}
        || $self->{_async_db}->{_workers_config}->{_count}
        || 2;

    # 2. Re-create the async engine
    my $new_async_db = DBIx::Class::Async->create_async_db(
        schema_class   => $self->schema_class,
        connect_info   => $self->{_async_db}->{_connect_info},
        workers        => $worker_count,
        loop           => $self->{_async_db}->{_loop},
        # Pass through other configs like metrics/retry if they exist
        enable_metrics => $self->{_async_db}->{_enable_metrics},
        enable_retry   => $self->{_async_db}->{_enable_retry},
    );

    # 3. Build the new schema object
    my $new_self = bless {
        %$self,
        _async_db      => $new_async_db,
        _sources_cache => {},
    }, ref $self;

    # 4. Re-attach a fresh storage wrapper
    $new_self->{_storage} = DBIx::Class::Async::Storage::DBI->new(
        schema   => $new_self,
        async_db => $new_async_db,
    );

    return $new_self;
}


############################################################################

sub deploy {
    my ($self, $sqlt_args, $dir) = @_;

    my $async_db = $self->{_async_db};

    return DBIx::Class::Async::_call_worker(
        $async_db, 'deploy', [ $sqlt_args, $dir ],
    )->then(sub {
        my ($res) = @_;

        # Return the result (usually { success => 1 } or similar)
        # or return $self if you want to allow chaining.
        return $res;
    });
}

sub disconnect {
    my $self = shift;

    if (ref $self->{_async_db} eq 'HASH') {
        # 1. Properly stop every worker in the array
        if (my $workers = $self->{_async_db}->{_workers}) {
            for my $worker (@$workers) {
                if (blessed($worker) && $worker->can('stop')) {
                    eval { $worker->stop };
                }
            }
        }

        # 2. Clear the internal hash contents to break any circular refs
        %{$self->{_async_db}} = ();
    }

    # 3. Remove the manager entirely
    delete $self->{_async_db};

    # 4. Flush the metadata cache
    $self->{_sources_cache} = {};

    return $self;
}

############################################################################

sub inflate_column {
    my ($self, $source_name, $column, $handlers) = @_;

    my $schema = $self->{_native_schema};

    my @known_sources = $schema->sources;
    warn "[PID $$] Parent Schema class: " . ref($schema) if ASYNC_TRACE;

    # Attempt lookup
    my $source = eval { $schema->source($source_name) };

    if (!$source) {
        warn "[PID $$] Source '$source_name' not found. Attempting force-load via resultset..."
            if ASYNC_TRACE;
        eval { $schema->resultset($source_name) };
        $source = eval { $schema->source($source_name) };
    }

    croak "Could not find result source for '$source_name' in Parent process."
        unless $source;

    # Apply the handlers to the Parent's schema instance
    my $col_info = $source->column_info($column);
    $source->add_column($column => {
        %$col_info,
        inflate => $handlers->{inflate},
        deflate => $handlers->{deflate},
    });

    # Registry for Parent-side inflation of results coming back from Worker
    $self->{_async_db}{_custom_inflators}{$source_name}{$column} = $handlers;
}

############################################################################

sub populate {
    my ($self, $source_name, $data) = @_;

    # 1. Standard Guard Clauses
    croak("Schema is disconnected")   unless $self->{_async_db};
    croak("source_name required")     unless defined $source_name;
    croak("data required")            unless defined $data;
    croak("data must be an arrayref") unless ref $data eq 'ARRAY';

    # 2. Delegate to ResultSet
    # This creates the RS and immediately triggers the bulk insert logic
    return $self->resultset($source_name)->populate($data);
}

############################################################################

sub register_class {
    my ($self, $source_name, $result_class) = @_;

    croak("source_name and result_class required")
        unless $source_name && $result_class;

    # 1. Load the class in the Parent process
    # We do this to extract metadata (columns, relationships)
    unless ($result_class->can('result_source_instance')) {
        eval "require $result_class";
        if ($@) {
            croak("Failed to load Result class '$result_class': $@");
        }
    }

    # 2. Get the ResultSource instance from the class
    # This contains the column definitions and table name
    my $source = eval { $result_class->result_source_instance };
    if ($@ || !$source) {
        croak("Class '$result_class' does not appear to be a valid DBIx::Class Result class");
    }

    # 3. Register the source
    # This will populate your { _sources_cache } or internal metadata map
    return $self->register_source($source_name, $source);
}

sub register_source {
    my ($self, $source_name, $source) = @_;

    # 1. Update Parent Instance
    $self->{_sources_cache}->{$source_name} = $source;

    # 2. Track this for Workers
    # Store the 'source' metadata so we can send it to workers if needed
    $self->{_dynamic_sources}->{$source_name} = $source;

    # 3. Class-level registration (for future local instances)
    my $schema_class = $self->{_schema_class};
    $schema_class->register_source($source_name, $source) if $schema_class;

    return $source;
}

sub resultset {
    my ($self, $source_name) = @_;

    unless (defined $source_name && length $source_name) {
        croak("resultset() requires a source name");
    }

    # 1. Check our cache for the source metadata
    # (In DBIC, a 'source' contains column info, class names, etc.)
    my $source = $self->{_sources_cache}{$source_name};

    unless ($source) {
        # Fetch metadata from the real DBIx::Class::Schema class
        $source = $self->_resolve_source($source_name);
        $self->{_sources_cache}{$source_name} = $source;
    }

    my $result_class = $self->class($source_name);
    # 2. Create the new Async ResultSet
    return DBIx::Class::Async::ResultSet->new(
        source_name     => $source_name,
        schema_instance => $self,              # Access to _record_metric
        async_db        => $self->{_async_db}, # Access to _call_worker
        result_class    => $result_class,
    );
}

############################################################################

sub set_default_context {
    my $self = shift;

    # No-op for compatibility with external libraries
    # that expect a standard DBIC Schema interface.
    # In an Async world, we avoid global context to prevent
    # cross-talk between event loop cycles.

    return $self;
}

sub schema_version {
    my $self  = shift;

    # Updated to match your actual internal state key
    my $class = $self->{_async_db}->{_schema_class};

    unless ($class) {
        croak("schema_class is not defined in " . ref($self));
    }

    # Use 'can' to safely check for the method on the class
    return $class->schema_version if $class->can('schema_version');

    return undef;
}

sub sync_metadata {
    my ($self) = @_;

    my $async_db = $self->{_async_db}; # Direct access
    my @futures;

    # Ping every worker in the pool
    for (1 .. $async_db->{_workers_config}->{_count}) {
        push @futures, DBIx::Class::Async::_call_worker($async_db, 'ping', {});
    }

    return Future->wait_all(@futures);
}

sub schema_class {
    my ($self) = @_;

    return $self->{_async_db}->{_schema_class};
}

sub source_ {
    my ($self, $source_name) = @_;

    unless (exists $self->{_sources_cache}{$source_name}) {
        my $source = eval { $self->{_native_schema}->source($source_name) };

        croak("No such source '$source_name'") if $@ || !$source;

        $self->{_sources_cache}{$source_name} = $source;
    }
    return $self->{_sources_cache}{$source_name};
}

sub sources_ {
    my $self = shift;
    return $self->{_native_schema}->sources;
}

sub storage {
    my $self = shift;
    return $self->{_storage}; # Your Async storage wrapper
}


sub source {
    my ($self, $source_name) = @_;

    # 1. Retrieve the cached entry
    my $cached = $self->{_sources_cache}{$source_name};

    # 2. Check if we need to (re)fetch:
    #    Either we have no entry, or it's a raw HASH (autovivification artifact)
    if (!$cached || !blessed($cached)) {

        # Clean up any "ghost" hash before re-fetching
        delete $self->{_sources_cache}{$source_name};

        # 3. Use the persistent provider to keep ResultSource objects alive
        $self->{_metadata_provider} ||= do {
            my $class = $self->{_async_db}->{_schema_class};
            eval "require $class" or die "Could not load schema class $class: $@";
            $class->connect(@{$self->{_async_db}->{_connect_info}});
        };

        # 4. Fetch the source and validate its blessing
        my $source_obj = eval { $self->{_metadata_provider}->source($source_name) };

        if (blessed($source_obj)) {
            $self->{_sources_cache}{$source_name} = $source_obj;
        } else {
            return undef;
        }
    }

    return $self->{_sources_cache}{$source_name};
}

sub sources {
    my $self = shift;

    my $schema_class = $self->{_async_db}->{_schema_class};
    my $connect_info = $self->{_async_db}->{_connect_info};
    my $temp_schema = $schema_class->connect(@{$connect_info});
    my @sources = $temp_schema->sources;

    $temp_schema->storage->disconnect;

    return @sources;
}

############################################################################

sub txn_begin {
    my $self = shift;

    # We return the future so the caller can wait for the 'BEGIN' to finish
    return DBIx::Class::Async::_call_worker(
        $self->{_async_db},
        'txn_begin',
        {}
    );
}

sub txn_commit {
    my $self = shift;

    return DBIx::Class::Async::_call_worker(
        $self->{_async_db},
        'txn_commit',
        {}
    );
}

sub txn_rollback {
    my $self = shift;

    return DBIx::Class::Async::_call_worker(
        $self->{_async_db},
        'txn_rollback',
        {}
    );
}

sub txn_do {
    my ($self, $steps) = @_;

    croak "txn_do requires an ARRAYREF of steps"
        unless ref $steps eq 'ARRAY';

    return DBIx::Class::Async::_call_worker(
        $self->{_async_db},
        'txn_do',
        $steps
    )->then(sub {
        my ($result) = @_;
        return Future->fail($result->{error}) if ref $result eq 'HASH' && $result->{error};
        return Future->done($result);
    });
}

sub txn_batch {
    my ($self, @args) = @_;

    croak "Async database handle not initialised in schema."
        unless $self->{_async_db};

    # Allow both txn_batch([$h1, $h2]) and txn_batch($h1, $h2)
    my @operations = (ref $args[0] eq 'ARRAY') ? @{$args[0]} : @args;

    # 1. Parent-side Validation
    for my $op (@operations) {
        croak "Each operation must be a hashref with 'type' key"
            unless (ref $op eq 'HASH' && $op->{type});

        if ($op->{type} =~ /^(update|delete|create)$/) {
            croak "Operation type '$op->{type}' requires 'resultset' parameter"
                unless $op->{resultset};
        }
    }

    # 2. Direct call to the worker
    return DBIx::Class::Async::_call_worker(
        $self->{_async_db},
        'txn_batch',
        \@operations
    )->then(sub {
        my ($result) = @_;

        # Ensure we handle the result correctly
        if (ref $result eq 'HASH' && $result->{error}) {
            return Future->fail($result->{error});
        }

        return Future->done($result);
    });
}

############################################################################

sub unregister_source {
    my ($self, $source_name) = @_;

    croak("source_name is required") unless defined $source_name;

    # 1. Reach into the manager hashref (the "Async DB" manager)
    my $class = $self->{_async_db}->{_schema_class};
    unless ($class) {
        croak("schema_class is not defined in manager for " . ref($self));
    }

    # 2. Local Cache Cleanup
    # Even if the file stays on disk, we prevent the Parent from
    # attempting to generate new ResultSets for this source.
    delete $self->{_sources_cache}->{$source_name};

    # 3. Class-Level Cleanup
    # This prevents any future workers (or re-initializations)
    # from seeing this source definition.
    if ($class->can('unregister_source')) {
        $class->unregister_source($source_name);
    }

    return $self;
}

############################################################################

sub _record_metric {
    my ($self, $type, $name, @args) = @_;

    # 1. Check if metrics are enabled via the async_db state
    # 2. Ensure the global $METRICS object exists
    return unless $self->{_async_db}
               && $self->{_async_db}{_enable_metrics}
               && defined $METRICS;

    # 3. Handle different metric types (parity with old design)
    if ($type eq 'inc') {
        # Usage: $schema->_record_metric('inc', 'query_count', 1)
        $METRICS->inc($name, @args);
    }
    elsif ($type eq 'observe') {
        # Usage: $schema->_record_metric('observe', 'query_duration', 0.05)
        $METRICS->observe($name, @args);
    }
    elsif ($type eq 'set') {
        # Usage: $schema->_record_metric('set', 'worker_pool_size', 5)
        $METRICS->set($name, @args);
    }

    return;
}

sub _resolve_source {
    my ($self, $source_name) = @_;

    croak "Missing source name." unless defined $source_name;

    my $schema_class = $self->{_async_db}{_schema_class};

    croak "Schema class not found." unless defined $schema_class;

    # 1. Ask the main DBIC Schema class for the source metadata
    # We call this on the class name, not an instance, to stay "light"
    my $source = eval { $schema_class->source($source_name) };

    if ($@ || !$source) {
        croak "Could not resolve source '$source_name' in $schema_class: $@";
    }

    # 2. Extract only what we need for the Async side
    return {
        result_class => $source->result_class,
        columns      => [ $source->columns ],
        relationships => {
            # We map relationships to know how to handle joins/prefetch later
            map { $_ => $source->relationship_info($_) } $source->relationships
        },
    };
}

sub _build_inflator_map {
    my ($self, $schema) = @_;

    my $map = {};
    foreach my $source_name ($schema->sources) {
        my $source = $schema->source($source_name);
        foreach my $col ($source->columns) {
            my $info = $source->column_info($col);

            # Extract both inflate and deflate coderefs
            if ($info->{deflate} || $info->{inflate}) {
                $map->{$source_name}{$col} = {
                    deflate => $info->{deflate},
                    inflate => $info->{inflate},
                };
            }
        }
    }

    return $map;
}

############################################################################

sub AUTOLOAD {
    my $self = shift;

    return unless ref $self;

    our $AUTOLOAD;
    my ($method) = $AUTOLOAD =~ /([^:]+)$/;

    return if $method eq 'DESTROY';

    if ($self->{_async_db} && exists $self->{_async_db}->{schema}) {
        my $real_schema = $self->{_async_db}->{schema};
        if ($real_schema->can($method)) {
            return $real_schema->$method(@_);
        }
    }

    croak "Method $method not found in " . ref($self);
}

1; # End of DBIx::Class::Async::Schema
