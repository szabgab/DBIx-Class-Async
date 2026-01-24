package DBIx::Class::Async;

use strict;
use warnings;
use utf8;
use feature 'state';
use CHI;
use Carp;
use Try::Tiny;
use IO::Async::Loop;
use IO::Async::Function;
use Time::HiRes qw(time);
use Digest::MD5 qw(md5_hex);
use Type::Params qw(compile);
use Scalar::Util qw(blessed);
use Types::Standard qw(Str ScalarRef HashRef ArrayRef Maybe Int CodeRef);

our $METRICS;

use constant {
    DEFAULT_WORKERS       => 4,
    DEFAULT_CACHE_TTL     => 300,
    DEFAULT_QUERY_TIMEOUT => 30,
    DEFAULT_RETRIES       => 3,
    HEALTH_CHECK_INTERVAL => 300,
};

use Data::Dumper;

my %HANDLERS = (
    count  => \&_count,
    #first  => \&_handle_row_query,
    #find   => \&_handle_row_query,
    #delete => \&_handle_action,
    #update => \&_handle_action,
);

sub _call_worker {
    my ($db, $operation, @args) = @_;

    warn "[PID $$] STAGE 3 (Parent): Calling worker for $operation";

    my $worker = _next_worker($db);
    warn "Found worker: ", ref($worker);

    # This returns a Future - make sure it's the right kind
    my $future = $worker->call(
        args => [
            $db->{_schema_class},
            $db->{_connect_info},
            $db->{_workers_config},
            $operation,
            @args,
        ],
    );

    warn "Returning future: ", ref($future);
    return $future;
}

sub count {
    my ($db, $payload) = @_;

    warn "[PID $$] STAGE 2 (Parent): Bridge - sending to worker";
    $db->{_stats}{_queries}++;

    # Call worker and return the Future directly
    my $future = _call_worker($db, 'count', $payload);
    warn "Count returning future: ", ref($future);
    return $future;
}

sub _init_workers {
    my $async_db = shift;

    for my $worker_id (1..$async_db->{_workers_config}{_count}) {
        my $worker = IO::Async::Function->new(
            code => sub {
                use strict;
                use warnings;
                use feature 'state';

                warn "[PID $$] Worker CODE block started";

                my ($schema_class, $connect_info, $worker_config, $operation, $payload, $inc_paths) = @_;

                warn "[PID $$] Worker received " . scalar(@_) . " arguments";
                warn "[PID $$] Schema class: $schema_class";
                warn "[PID $$] Operation: $operation";

                # Add parent's @INC to worker's @INC
                if ($inc_paths && ref $inc_paths eq 'ARRAY') {
                    warn "[PID $$] Adding " . scalar(@$inc_paths) . " paths to \@INC";
                    unshift @INC, @$inc_paths;
                    warn "[PID $$] Worker \@INC now has " . scalar(@INC) . " entries";
                } else {
                    warn "[PID $$] WARNING: No inc_paths received!";
                }

                warn "[PID $$] STAGE 4 (Worker): Received operation: $operation";

                # Create or reuse schema connection
                state $schema_cache = {};
                my $pid = $$;

                warn "[PID $$] Checking schema cache for PID $pid";

                unless (exists $schema_cache->{$pid}) {
                    warn "[PID $$] Worker initializing new schema connection";
                    warn "[PID $$] About to require $schema_class";

                    # Load schema class in worker process
                    my $require_result = eval "require $schema_class; 1";
                    if (!$require_result || $@) {
                        my $err = $@ || 'Unknown error';
                        warn "[PID $$] FAILED to load schema class: $err";
                        warn "[PID $$] \@INC is: " . join(", ", @INC);
                        die "Worker Load Fail: $err";
                    }

                    warn "[PID $$] Schema class loaded successfully";

                    unless ($schema_class->can('connect')) {
                        warn "[PID $$] Schema class has no 'connect' method!";
                        die "Schema class $schema_class does not provide 'connect' method";
                    }

                    warn "[PID $$] Attempting database connection...";

                    # Connect to database
                    my $schema = eval { $schema_class->connect(@$connect_info); };
                    if ($@) {
                        warn "[PID $$] Database connection FAILED: $@";
                        die "Failed to connect to database: $@";
                    }
                    unless (defined $schema) {
                        warn "[PID $$] Schema connection returned undef!";
                        die "Schema connection returned undef";
                    }

                    warn "[PID $$] Database connected successfully";

                    $schema_cache->{$pid} = $schema;

                    warn "[PID $$] Worker initialization complete";
                }

                warn "[PID $$] STAGE 5 (Worker): Executing operation: $operation";

                my $result;
                eval {
                    my $schema = $schema_cache->{$pid};

                    warn "[PID $$] Schema from cache: " . (defined $schema ? ref($schema) : "UNDEF");

                    if ($operation eq 'count') {
                        warn "[PID $$] STAGE 6 (Worker): Performing count";

                        my $source_name = $payload->{source_name};
                        my $cond = $payload->{cond} || {};
                        my $attrs = $payload->{attrs} || {};

                        warn "[PID $$] Building resultset for source: $source_name";

                        my $rs = $schema->resultset($source_name)->search($cond, $attrs);

                        warn "[PID $$] Executing count...";
                        $result = $rs->count;

                        warn "[PID $$] Count complete: $result";
                    }
                    else {
                        die "Unknown operation: $operation";
                    }
                };

                if ($@) {
                    warn "[PID $$] Worker execution error: $@";
                    die $@;
                }

                warn "[PID $$] Worker returning: $result";
                return $result;
            },
            max_workers => 1,
        );

        $async_db->{_loop}->add($worker);

        push @{$async_db->{_workers}}, {
            instance => $worker,
            healthy => 1,
            pid => undef,
        };
    }
}

sub _handle_row_query {
    my ($schema, $op, $payload) = @_;

    my $rs  = $schema->resultset($payload->{source_name})->search($payload->{cond}, $payload->{attrs});
    my $row = $rs->$op(); # Calls ->first() or ->find()

    return unless $row;

    # Convert the live object into a "transportable" hashref
    return { $row->get_columns };
}

sub _next_worker {
    my ($db) = @_;

    my $idx    = $db->{_worker_idx};
    my $worker = $db->{_workers}[$idx];

    $db->{_worker_idx} = ($idx + 1) % @{$db->{_workers}};

    return $worker->{instance};
}

sub disconnect_async_db {
    my ($async_db) = @_;

    return unless $async_db && ref $async_db eq 'HASH';

    # 1. Clear the health check timer
    if ($async_db->{_health_check_timer}) {
        $async_db->{_loop}->remove($async_db->{_health_check_timer});
        delete $async_db->{_health_check_timer};
    }

    # 2. Shutdown workers
    if ($async_db->{_workers}) {
        foreach my $worker_info (@{ $async_db->{_workers} }) {
            if (my $instance = $worker_info->{instance}) {
                $async_db->{_loop}->remove($instance);
            }
        }
        $async_db->{_workers} = [];
    }

    # 3. Final state update
    $async_db->{_is_connected} = 0;

    return 1;
}

sub create_async_db {
    my ($class, %args) = @_;

    my $schema_class = $args{schema_class} or croak "schema_class required";
    my $connect_info = $args{connect_info} or croak "connect_info required";
    my $workers = $args{workers} || DEFAULT_WORKERS;

    unless (eval { $schema_class->can('connect') } || eval "require $schema_class") {
        croak "Cannot load schema class $schema_class: $@";
    }

    # Preserving your TTL logic exactly
    my $cache_ttl = $args{cache_ttl};
    if (defined $cache_ttl) {
        $cache_ttl = undef if $cache_ttl == 0;
    } else {
        $cache_ttl = DEFAULT_CACHE_TTL;
    }

    # Accept BOTH 'loop' and 'async_loop' for compatibility
    my $loop = $args{loop} || $args{async_loop} || IO::Async::Loop->new;

    my $async_db = {
        _schema_class => $schema_class,
        _connect_info => $connect_info,
        _loop => $loop,  # ← Make sure this is set
        _workers => [],
        _workers_config => {
            _count => $workers,
            _query_timeout => $args{query_timeout} || DEFAULT_QUERY_TIMEOUT,
            _on_connect_do => $args{on_connect_do} || [],
        },
        _cache => $args{cache} || _build_default_cache($cache_ttl),
        _cache_ttl => $cache_ttl,
        _enable_retry => $args{enable_retry} // 0,
        _retry_config => {
            _max_retries => $args{max_retries} || DEFAULT_RETRIES,
            _delay => $args{retry_delay} || 1,
            _factor => 2,
        },
        _enable_metrics => $args{enable_metrics} // 0,
        _is_connected => 1,
        _worker_idx => 0,
        _stats => {
            _queries => 0,
            _errors => 0,
            _cache_hits => 0,
            _cache_misses => 0,
            _deadlocks => 0,
            _retries => 0,
        },
    };

    _init_metrics($async_db) if $async_db->{enable_metrics};
    _init_workers($async_db);

    if (my $interval = $args{health_check} // HEALTH_CHECK_INTERVAL) {
        _start_health_checks($async_db, $interval);
    }

    return $async_db;
}

sub _init_metrics {
    my $async_db = shift;

    # Try to load Metrics::Any
    eval {
        require Metrics::Any;
        Metrics::Any->import('$METRICS');

        # Initialise metrics
        $METRICS->make_counter('db_async_queries_total');
        $METRICS->make_counter('db_async_cache_hits_total');
        $METRICS->make_counter('db_async_cache_misses_total');
        $METRICS->make_histogram('db_async_query_duration_seconds');
        $METRICS->make_gauge('db_async_workers_active');

    };

    # Silently ignore if Metrics::Any is not available
    if ($@) {
        $async_db->{_enable_metrics} = 0;
        undef $METRICS;
    }
}

sub _start_health_checks {
    my ($async_db, $interval) = @_;

    return if $interval <= 0;

    # Try to create the timer
    eval {
        $async_db->{_health_check_timer} = $async_db->{_loop}->repeat(
            interval => $interval,
            code     => sub {
                # Don't use async here - just fire and forget
                _health_check($async_db)->retain;
            },
        );
    };

    if ($@) {
        # If repeat fails, try a different approach or disable health checks
        warn "Failed to start health checks: $@" if $ENV{DBIC_ASYNC_DEBUG};
    }
}

sub _health_check {
    my $async_db = shift;

    my @checks = map {
        my $worker_info = $_;
        my $worker = $worker_info->{instance};
        $worker->call(
            args => [
                $async_db->{_schema_class},
                $async_db->{_connect_info},
                $async_db->{_workers_config},
                'health_check',
            ],
            timeout => 5,
        )->then(sub {
            $worker_info->{healthy} = 1;
            return Future->done(1);
        }, sub {
            $worker_info->{healthy} = 0;
            return Future->done(0);
        })
    } @{$async_db->{_workers}};

    return Future->wait_all(@checks)->then(sub {
        my @results = @_;
        my $healthy_count = grep { $_->get } @results;

        _record_metric($async_db, 'set', 'db_async_workers_active', $healthy_count);

        return Future->done($healthy_count);
    });
}

sub _record_metric {
    my ($async_db, $type, $name, @args) = @_;

    return unless $async_db->{_enable_metrics} && defined $METRICS;

    if ($type eq 'inc') {
        $METRICS->inc($name, @args);
    } elsif ($type eq 'observe') {
        $METRICS->observe($name, @args);
    } elsif ($type eq 'set') {
        $METRICS->set($name, @args);
    }
}


sub _build_default_cache {
    my ($ttl) = @_;

    my %params = (
        driver => 'Memory',
        global => 1,
    );

    # Add expires_in only if ttl is defined (undef means never expire in CHI)
    $params{expires_in} = $ttl if defined $ttl;

    return CHI->new(%params);
}

package DBIx::Class::Async::Schema;

use strict;
use warnings;
use utf8;

use Carp;
use Future;
use Try::Tiny;
use Scalar::Util 'blessed';

 sub connect {
     my ($class, @args) = @_;

     # Separate async options from connect_info
     my $async_options = {};
     if (ref $args[-1] eq 'HASH' && !exists $args[-1]->{RaiseError}) {
         $async_options = pop @args;
     }

     my $schema_class = $async_options->{schema_class}
        or croak "schema_class is required in async options";

     # Validation logic stays exactly as is
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
     warn "In Schema.pm, connect()";


     if ($@) {
         croak "Failed to create async engine: $@";
     }

     my $self = bless {
         _async_db      => $async_db,
         _sources_cache => {},
     }, $class;

     # Storage plumbing
     my $storage = DBIx::Class::Async::Storage::DBI->new(
         schema   => $self,
         async_db => $async_db,
     );

     $self->{_storage} = $storage;

     return $self;
}

sub resultset {
    my ($self, $source_name) = @_;

    croak "resultset() requires a source name" unless $source_name;

    return DBIx::Class::Async::ResultSet->new(
        schema      => $self->{_async_db}->{_schema_class},
        async_db    => $self->{_async_db},
        source_name => $source_name,
    );
}



package DBIx::Class::Async::ResultSet;

use strict;
use warnings;
use utf8;
use v5.14;

use Carp;
use Future;
use Scalar::Util 'blessed';



use Data::Dumper;

sub new {
    my ($class, %args) = @_;

    # 1. Validation
    croak "Missing required argument: schema"      unless $args{schema};
    croak "Missing required argument: async_db"    unless $args{async_db};
    croak "Missing required argument: source_name" unless $args{source_name};

    # 2. Internal blessing
    return bless {
        _schema        => $args{schema},
        _async_db      => $args{async_db},
        _source_name   => $args{source_name},
        _result_class  => $args{result_class},
        _source        => undef,
        _cond          => $args{cond}  || {},
        _attrs         => $args{attrs} || {},
        _rows          => undef,
        _pos           => 0,
        _pager         => $args{pager} || undef,
        _entries       => $args{entries}       || undef,
        _is_prefetched => $args{is_prefetched} || 0,
     }, $class;
}

sub new_result_set {
    my ($self, $args) = @_;

    my $class = ref $self;

    # 2. Inherit the Async Bridge and the Source context
    $args->{async_db}    //= $self->{_async_db};
    $args->{source_name} //= $self->{_source_name};
    $args->{schema}      //= $self->{_schema};

    # 3. Handle result_class inheritance
    # If a new one isn't provided, keep the parent's
    $args->{result_class} //= $self->{_result_class};

    # 4. Standard DBIC state defaults for new ResultSets
    $args->{cond}  //= {};
    $args->{attrs} //= {};

    # 5. Bless the new object into our Async class
    return bless $args, $class;
}

sub _build_payload {
    my ($self, $cond, $attrs) = @_;

    # 1. Base Merge
    my $merged_cond  = { %{ $self->{_cond}  || {} }, %{ $cond  || {} } };
    my $merged_attrs = { %{ $self->{_attrs} || {} }, %{ $attrs || {} } };

    # 2. The "Slice" Special Case
    # DBIC requires subquery wrappers for counts on results with limits/offsets
    if ( $merged_attrs->{rows} || $merged_attrs->{offset} || $merged_attrs->{limit} ) {
        $merged_attrs->{alias}       //= 'subquery_for_count';
        $merged_attrs->{is_subquery} //= 1;
    }

    # 3. Future Special Cases (Reserved)
    # This is where you'd handle things like custom 'join' logic
    # or ensuring 'order_by' is stripped for simple counts to save CPU.

    return {
        source_name => $self->{_source_name},
        cond        => $merged_cond,
        attrs       => $merged_attrs,
    };
}

sub count {
    my ($self, $cond, $attrs) = @_;

    my $db = $self->{_async_db};
    $db->{_stats}{_queries}++;

    my $payload = $self->_build_payload($cond, $attrs);

    warn "[PID $$] STAGE 1 (Parent): Dispatching count";

    # This returns a Future that will be resolved by the worker
    return DBIx::Class::Async::count($db, $payload);
}

sub search {
    my ($self, $cond, $attrs) = @_;

    # Handle the condition merging carefully
    my $new_cond;

    # 1. If the new condition is a literal (Scalar/Ref), it overrides/becomes the condition
    if (ref $cond eq 'REF' || ref $cond eq 'SCALAR') {
        $new_cond = $cond;
    }

    # 2. If the current existing condition is a literal,
    # and we try to add a hash, we usually want to encapsulate or override.
    # For now, let's allow the new condition to take precedence if it's a hash.
    elsif (ref $cond eq 'HASH') {
        if (ref $self->{_cond} eq 'HASH' && keys %{$self->{_cond}}) {
            # ONLY use -and if we actually have two sets of criteria to join
            $new_cond = { -and => [ $self->{_cond}, $cond ] };
        }
        else {
            # If the current condition is empty/undef, just use the new one
            $new_cond = $cond;
        }
    }
    else {
        # Fallback for simple cases or undef
        $new_cond = $cond || $self->{_cond};
    }

    my $merged_attrs = { %{$self->{_attrs} || {}}, %{$attrs || {}} };

    return $self->new_result_set({
        cond         => $new_cond,
        attrs        => $merged_attrs,
        # Only reset these if they aren't in the merged attributes
        result_class  => $attrs->{result_class} // $self->{_result_class},
        _rows         => $merged_attrs->{rows}  // undef,
        _pos          => 0,
        _pager        => undef,
        entries       => undef,
        is_prefetched => 0,
    });
}

sub as_query {
    my $self = shift;

    my $bridge       = $self->{_async_db};
    my $schema_class = $bridge->{_schema_class};

    unless ($schema_class->can('resultset')) {
        eval "require $schema_class" or die "as_query: $@";
    }

    # Silence the "Generic Driver" warnings for the duration of this method
    local $SIG{__WARN__} = sub {
        warn @_ unless $_[0] =~ /undetermined_driver|sql_limit_dialect|GenericSubQ/
    };

    unless ($bridge->{_metadata_schema}) {
        $bridge->{_metadata_schema} = $schema_class->connect('dbi:NullP:');
    }

    # SQL is generated lazily; warnings often trigger here or at as_query()
    my $real_rs = $bridge->{_metadata_schema}
                         ->resultset($self->{_source_name})
                         ->search($self->{_cond}, $self->{_attrs});

    return $real_rs->as_query;
}


package DBIx::Class::Async::Storage;

sub new {
    my ($class, %args) = @_;

    # Standard DBIC storage expects a reference to the schema
    my $self = bless {
        _schema   => $args{schema},
        _async_db => $args{async_db}, # The worker pool engine
    }, $class;

    # WEAKEN the schema reference to prevent circular memory leaks
    # that caused your "5 vs 2 processes" test failure.
    weaken($self->{_schema}) if $self->{_schema};

    return $self;
}

sub dbh {
    my $self = shift;
    # In Async mode, the parent process doesn't hold a DBH.
    return undef;
}

sub schema {
    my $self = shift;
    return $self->{_schema};
}

sub disconnect {
    my $self = shift;

    # Delegate cleanup to the functional library
    if ($self->{_async_db}) {
        DBIx::Class::Async::disconnect_async_db($self->{_async_db});
    }

    return 1;
}

package DBIx::Class::Async::Storage::DBI;

use strict;
use warnings;
use base 'DBIx::Class::Async::Storage';

sub dbh {
     my $self = shift;
     # In Async mode, the parent process doesn't hold a DBH.
     # The workers hold the DBHs.
     return undef;
}

sub cursor {
    my ($self, $rs) = @_;

    # Just like DBIC, we return a DBI-specific cursor
    return DBIx::Class::Async::Storage::DBI::Cursor->new(
        storage => $self,
        rs      => $rs,
    );
}

sub debug {
    my ($self, $level) = @_;
    return $self->{debug} = $level if defined $level;
    return $self->{debug} || 0;
}

package main;

use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);
use Scalar::Util qw(blessed);  # ← ADD THIS

use Data::Dumper;

# The pieces we've built
use TestSchema;

# 1. Setup the physical environment
my $loop = IO::Async::Loop->new;
my ($fh, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);
close $fh;  # ← ADD THIS - close the filehandle so SQLite can use it
my $dsn = "dbi:SQLite:dbname=$db_file";

note "Database file: $db_file";

# 2. Deploy the database (Synchronously, just for setup)
my $setup_schema = TestSchema->connect($dsn);
$setup_schema->deploy();
$setup_schema->resultset('User')->create({ name => 'Alice', active => 1 });
$setup_schema->resultset('User')->create({ name => 'Bob',   active => 1 });

# Verify the setup worked
my $setup_count = $setup_schema->resultset('User')->count;
note "Setup created $setup_count users";

# Disconnect the setup schema
$setup_schema->storage->disconnect;

# --- THE ASYNC START ---

# 3. Create the Async Schema Object
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    loop         => $loop,  # ← Use 'loop' not 'async_loop'
    workers      => 2,
});

# 4. Get the Async ResultSet
my $rs = $async_schema->resultset('User');

# 5. Execute the First Count
note "Dispatching async count request...";
my $future = $rs->count({ active => 1 });

note "Future type: " . ref($future);

# 6. The loop needs to RUN to process the worker response
# Don't use await() - use the loop to run until the future is ready
my $count;
$future->on_ready(sub {
    my $f = shift;
    $count = $f->get;
    $loop->stop;
});

# Run the loop until the future completes
$loop->run;

note "Got count: " . (defined $count ? $count : 'undef');

# 7. Verify
is($count, 2, "The async count returned the correct number of rows via the worker pool");

done_testing();
