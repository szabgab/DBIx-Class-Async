package DBIx::Class::Async;

use strict;
use warnings;
use utf8;

use v5.14;

use CHI;
use Carp;
use Try::Tiny;
use IO::Async::Loop;
use IO::Async::Function;
use Time::HiRes qw(time);
use Digest::MD5 qw(md5_hex);
use Type::Params qw(compile);
use Scalar::Util qw(blessed);
use DBIx::Class::Async::Row;
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

sub _next_worker {
    my ($db) = @_;

    return unless $db->{_workers} && @{$db->{_workers}};

    $db->{_worker_idx} //= 0;

    die "No workers available" unless $db->{_workers} && @{$db->{_workers}};

    my $idx    = $db->{_worker_idx};
    my $worker = $db->{_workers}[$idx];

    $db->{_worker_idx} = ($idx + 1) % @{$db->{_workers}};

    return $worker->{instance};
}

sub _call_worker {
     my ($db, $operation, @args) = @_;

     warn "[PID $$] STAGE 3 (Parent): Calling worker for $operation";

     my $worker = _next_worker($db);

     # 1. Start the call
     return $worker->call(
         args => [
             $db->{_schema_class},
             $db->{_connect_info},
             $db->{_workers_config},
             $operation,
             @args,
         ],
     )->then(sub {
         my $result = shift;

         # 2. Check for worker-side caught errors
         if (ref($result) eq 'HASH' && exists $result->{error}) {
             $db->{_stats}->{_errors}++;
             # Transform this "done" into a "fail"
             return Future->fail($result->{error});
         }

         # 3. Handle actual success
         $db->{_stats}->{_queries}++;
         return Future->done($result);
     }, sub {
         # 4. Handle process-level crashes (e.g., worker died)
         my ($error) = @_;
         $db->{_stats}->{_errors}++;
         return Future->fail($error);
     });
}

sub delete {
    my ($db, $payload) = @_;
    warn "[PID $$] Bridge - sending 'delete' to worker";
    return _call_worker($db, 'delete', $payload);
}

sub create {
    my ($db, $payload) = @_;
    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'create' to worker";

    return _call_worker($db, 'create', $payload);
}

sub update {
    my ($db, $payload) = @_;
    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'update' to worker";

    return _call_worker($db, 'update', $payload);
}

sub all {
    my ($db, $payload) = @_;
    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'all' to worker";

    return _call_worker($db, 'all', $payload);
}

sub count {
    my ($db, $payload) = @_;

    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'count' to worker";

    return _call_worker($db, 'count', $payload);
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
    my $workers      = $args{workers} || DEFAULT_WORKERS;

    unless (eval { $schema_class->can('connect') } || eval "require $schema_class") {
        croak "Cannot load schema class $schema_class: $@";
    }

    # Preserving your TTL logic exactly
    my $cache_ttl = $args{cache_ttl};
    if (defined $cache_ttl) {
        $cache_ttl = undef if $cache_ttl == 0;
    }
    else {
        $cache_ttl = DEFAULT_CACHE_TTL;
    }

    # This is the "plain hashref" - No bless!
    my $async_db = {
        _schema_class     => $schema_class,
        _connect_info     => $connect_info,
        _loop             => $args{loop} || IO::Async::Loop->new,
        _workers          => [],
        _workers_config   => {
            _count          => $workers,
            _query_timeout  => $args{query_timeout} || DEFAULT_QUERY_TIMEOUT,
            _on_connect_do  => $args{on_connect_do} || [],
        },
        _cache            => $args{cache} || _build_default_cache($cache_ttl),
        _cache_ttl        => $cache_ttl,
        _enable_retry     => $args{enable_retry} // 0,
        _retry_config     => {
            _max_retries  => $args{max_retries} || DEFAULT_RETRIES,
            _delay        => $args{retry_delay} || 1,
            _factor       => 2,
        },
        _enable_metrics   => $args{enable_metrics} // 0,
        _is_connected     => 1,
        _worker_idx       => 0,
        _stats            => {
            _queries      => 0,
            _errors       => 0,
            _cache_hits   => 0,
            _cache_misses => 0,
            _deadlocks    => 0,
            _retries      => 0,
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

sub _init_workers {
    my $async_db = shift;

    for my $worker_id (1..$async_db->{_workers_config}{_count}) {
        my $worker = IO::Async::Function->new(
            code => sub {
                use strict;
                use warnings;
                use feature 'state';

                warn "[PID $$] Worker CODE block started";

                my ($schema_class, $connect_info, $worker_config, $operation, $payload) = @_;

                warn "[PID $$] Worker received " . scalar(@_) . " arguments";
                warn "[PID $$] Schema class: $schema_class";
                warn "[PID $$] Operation: $operation";
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

                    if ($operation =~ /^(count|sum|max|min|avg|average)$/) {
                        warn "[PID $$] STAGE 6 (Worker): Performing aggregate $operation";

                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond}  || {};
                        my $attrs       = $payload->{attrs} || {};
                        my $column      = $payload->{column};

                        my $rs = $schema->resultset($source_name)->search($cond, $attrs);

                        # Use eval to catch DBIC errors (e.g., column doesn't exist)
                        my $val = eval {
                            if ($operation eq 'count') {
                                return $column ? $rs->get_column($column)->func('COUNT') : $rs->count;
                            }

                            if ($operation =~ /^(avg|average)$/) {
                                return $rs->get_column($column)->func('AVG');
                            }

                            return $rs->get_column($column)->$operation;
                        };

                        if ($@) {
                            warn "[PID $$] WORKER ERROR: $@";
                            $result = { error => $@ };
                        } else {
                            # IMPORTANT: Force to scalar to avoid HASH(0x...) in Parent
                            # This stringifies potential Math::BigInt objects or references
                            $result = defined $val ? "$val" : undef;
                            warn "[PID $$] $operation complete: $result";
                        }
                    }
                    elsif ($operation eq 'search') {
                        warn "[PID $$] STAGE 6 (Worker): Performing search";

                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond}  || {};
                        my $attrs       = $payload->{attrs} || {};
                        my $rs = $schema->resultset($source_name)->search($cond, $attrs);

                        # Execute and flatten to simple data (no objects!)
                        $rs->result_class($attrs->{result_class} || 'DBIx::Class::ResultClass::HashRefInflator');

                        my @rows = $rs->all;
                        $result  = \@rows;
                    }
                    elsif ($operation eq 'all') {
                        warn "[PID $$] STAGE 6 (Worker): Performing 'all' (search)";

                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond};
                        my $attrs       = $payload->{attrs};

                        my $rs = $schema->resultset($source_name)->search($cond, $attrs);

                        # Execute and flatten to simple data (no objects!)
                        $rs->result_class($attrs->{result_class} || 'DBIx::Class::ResultClass::HashRefInflator');

                        my @rows = $rs->all;
                        $result  = \@rows;
                    }
                    elsif ($operation eq 'update') {
                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond};
                        my $updates     = $payload->{updates};

                        if (!$updates || !keys %$updates) {
                            $result = 0;
                        } else {
                            $result = $schema->resultset($source_name)
                                             ->search($cond)
                                             ->update($updates);
                        }
                    }
                    elsif ($operation eq 'create') {
                        my $source_name = $payload->{source_name};
                        my $data        = $payload->{data};

                        # Perform the actual DBIC insert
                        my $row = $schema->resultset($source_name)->create($data);

                        # IMPORTANT: Return the inflated columns so the Parent gets
                        # the Auto-Increment ID and any DB-side defaults.
                        $result = { $row->get_inflated_columns };
                    }
                    elsif ($operation eq 'delete') {
                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond};

                        # Direct delete on the resultset matching the condition
                        $result = $schema->resultset($source_name)->search($cond)->delete + 0;
                    }
                    else {
                        die "Unknown operation: $operation";
                    }
                };

                if ($@) {
                    warn "[PID $$] Worker execution error: $@";
                    die $@;
                }

                warn "[PID $$] Worker returning result type: " . ref($result);
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


1; # End of DBIx::Class::Async
