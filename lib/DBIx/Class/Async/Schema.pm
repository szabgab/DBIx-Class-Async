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

    # Populate the inflator map
    $async_db->{_custom_inflators} = $class->_build_inflator_map($native_schema);

    my $self = bless {
        _async_db      => $async_db,
        _native_schema => $native_schema,
        _sources_cache => {},
    }, $class;

    my $storage = DBIx::Class::Async::Storage::DBI->new(
        schema   => $self,
        async_db => $async_db,
    );

    $self->{_storage} = $storage;

    return $self;
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

sub inflate_column {
    my ($self, $source_name, $column, $handlers) = @_;

    my $schema = $self->{_native_schema};

    my @known_sources = $schema->sources;
    warn "[PID $$] Parent Schema class: " . ref($schema);

    # Attempt lookup
    my $source = eval { $schema->source($source_name) };

    if (!$source) {
        warn "[PID $$] Source '$source_name' not found. Attempting force-load via resultset...";
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

    my $schema_class = $self->{_async_db}{_schema_class};

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

sub storage {
    my $self = shift;
    return $self->{_storage};
}

sub resultset {
    my ($self, $source_name) = @_;

    # 1. Check our cache for the source metadata
    # (In DBIC, a 'source' contains column info, class names, etc.)
    my $source = $self->{_sources_cache}{$source_name};

    unless ($source) {
        # Fetch metadata from the real DBIx::Class::Schema class
        $source = $self->_resolve_source($source_name);
        $self->{_sources_cache}{$source_name} = $source;
    }

    # 2. Create the new Async ResultSet
    return DBIx::Class::Async::ResultSet->new(
        source_name     => $source_name,
        schema_instance => $self,              # Access to _record_metric
        async_db        => $self->{_async_db}, # Access to _call_worker
        result_class    => $source->{result_class} || 'DBIx::Class::Core',
    );
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

sub _build_inflator_map {
    my ($class, $schema) = @_;

    my $map = {};
    foreach my $source_name ($schema->sources) {
        warn "[DEBUG] Scanning source: $source_name";
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
