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

use constant ASYNC_TRACE => $ENV{ASYNC_TRACE} || 0;
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

    my $worker = _next_worker($db);

    my $future = $worker->call(
        args => [
            $db->{_schema_class},
            $db->{_connect_info},
            $db->{_workers_config},
            $operation,
            @args,
        ],
    );

    # Use followed_by which handles both nested and non-nested Futures
    return $future->followed_by(sub {
        my ($f) = @_;

        # Handle failure
        if ($f->is_failed) {
            $db->{_stats}->{_errors}++;
            return Future->fail($f->failure);
        }

        # Get the result
        my $result = ($f->get)[0];

        # If result is itself a Future, flatten it
        if (Scalar::Util::blessed($result) && $result->isa('Future')) {
            return $result->followed_by(sub {
                my ($inner_f) = @_;

                if ($inner_f->is_failed) {
                    $db->{_stats}->{_errors}++;
                    return Future->fail($inner_f->failure);
                }

                my $inner_result = ($inner_f->get)[0];

                # Check for worker errors
                if (ref($inner_result) eq 'HASH' && exists $inner_result->{error}) {
                    $db->{_stats}->{_errors}++;
                    return Future->fail($inner_result->{error});
                }

                $db->{_stats}->{_queries}++;
                return Future->done($inner_result);
            });
        }

        # Not a nested Future - handle normally
        # Check for worker errors
        if (ref($result) eq 'HASH' && exists $result->{error}) {
            $db->{_stats}->{_errors}++;
            return Future->fail($result->{error});
        }

        $db->{_stats}->{_queries}++;
        return Future->done($result);
    });
}

sub delete {
    my ($db, $payload) = @_;
    warn "[PID $$] Bridge - sending 'delete' to worker" if ASYNC_TRACE;
    return _call_worker($db, 'delete', $payload);
}

sub create {
    my ($db, $payload) = @_;
    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'create' to worker"
        if ASYNC_TRACE;

    return _call_worker($db, 'create', $payload);
}

sub update {
    my ($db, $payload) = @_;
    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'update' to worker"
        if ASYNC_TRACE;

    return _call_worker($db, 'update', $payload);
}

sub all {
    my ($db, $payload) = @_;
    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'all' to worker"
        if ASYNC_TRACE;

    return _call_worker($db, 'all', $payload);
}

sub count {
    my ($db, $payload) = @_;

    warn "[PID $$] STAGE 2 (Parent): Bridge - sending 'count' to worker"
        if ASYNC_TRACE;

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

    # 1. Extract Column Metadata (Inflators/Deflators)
    # We do this before creating the hashref so we can include it
    my $custom_inflators = {};
    if ($schema_class->can('sources')) {
        foreach my $source_name ($schema_class->sources) {
            my $source = $schema_class->source($source_name);
            foreach my $col ($source->columns) {
                my $info = $source->column_info($col);
                if ($info->{deflate} || $info->{inflate}) {
                    $custom_inflators->{$source_name}{$col} = {
                        deflate => $info->{deflate},
                        inflate => $info->{inflate},
                    };
                }
            }
        }
    }

    # 2. Build the async_db state hashref
    my $async_db = {
        _schema_class      => $schema_class,
        _connect_info      => $connect_info,
        _custom_inflators  => $custom_inflators,
        _loop              => $args{loop} || IO::Async::Loop->new,
        _workers           => [],
        _workers_config    => {
            _count         => $workers,
            _query_timeout => $args{query_timeout} || DEFAULT_QUERY_TIMEOUT,
            _on_connect_do => $args{on_connect_do} || [],
        },
        _cache             => $args{cache} || _build_default_cache($cache_ttl),
        _cache_ttl         => $cache_ttl,
        _enable_retry      => $args{enable_retry} // 0,
        _retry_config      => {
            _max_retries   => $args{max_retries} || DEFAULT_RETRIES,
            _delay         => $args{retry_delay} || 1,
            _factor        => 2,
        },
        _enable_metrics    => $args{enable_metrics} // 0,
        _is_connected      => 1,
        _worker_idx        => 0,
        _query_cache       => {},
        _stats             => {
            _queries       => 0,
            _errors        => 0,
            _cache_hits    => 0,
            _cache_misses  => 0,
            _deadlocks     => 0,
            _retries       => 0,
        },
    };

    _init_metrics($async_db) if $async_db->{_enable_metrics};
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
        require IO::Async::Timer::Periodic;

        my $timer = IO::Async::Timer::Periodic->new(
            interval => $interval,
            on_tick  => sub {
                # Don't use async here - just fire and forget
                _health_check($async_db)->retain;
            },
        );

        $async_db->{_loop}->add($timer);
        $timer->start;

        $async_db->{_health_check_timer} = $timer;
    };

    if ($@) {
        # If repeat fails, try a different approach or disable health checks
        warn "Failed to start health checks: $@" if ASYNC_TRACE;
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


                my ($schema_class, $connect_info, $worker_config, $operation, $payload) = @_;

                if (ASYNC_TRACE) {
                    warn "[PID $$] Worker CODE block started";
                    warn "[PID $$] Worker received " . scalar(@_) . " arguments";
                    warn "[PID $$] Schema class: $schema_class";
                    warn "[PID $$] Operation: $operation";
                    warn "[PID $$] STAGE 4 (Worker): Received operation: $operation";
                }

                my $deflator;
                $deflator = sub {
                    my ($data) = @_;
                    return $data unless defined $data;

                    if ( eval { $data->isa('DBIx::Class::Row') } ) {
                        my %cols = $data->get_inflated_columns;
                        # Recurse for prefetched relations
                        foreach my $k (keys %cols) {
                            if (ref $cols{$k}) { $cols{$k} = $deflator->($cols{$k}) }
                        }
                        return \%cols;
                    }
                    if ( eval { $data->isa('DBIx::Class::ResultSet') } ) {
                        return [ map { $deflator->($_) } $data->all ];
                    }
                    if ( ref($data) eq 'ARRAY' ) {
                        return [ map { $deflator->($_) } @$data ];
                    }
                    return $data;
                };

                # Create or reuse schema connection
                state $schema_cache = {};
                my $pid = $$;

                warn "[PID $$] Checking schema cache for PID $pid" if ASYNC_TRACE;

                unless (exists $schema_cache->{$pid}) {
                    if (ASYNC_TRACE) {
                        warn "[PID $$] Worker initializing new schema connection";
                        warn "[PID $$] About to require $schema_class";
                    }


                    # Load schema class in worker process
                    my $require_result = eval "require $schema_class; 1";
                    if (!$require_result || $@) {
                        my $err = $@ || 'Unknown error';
                        warn "[PID $$] FAILED to load schema class: $err"
                            if ASYNC_TRACE;
                        die "Worker Load Fail: $err";
                    }

                    warn "[PID $$] Schema class loaded successfully"
                        if ASYNC_TRACE;

                    unless ($schema_class->can('connect')) {
                        warn "[PID $$] Schema class has no 'connect' method!"
                            if ASYNC_TRACE;
                        die "Schema class $schema_class does not provide 'connect' method";
                    }

                    warn "[PID $$] Attempting database connection..."
                        if ASYNC_TRACE;

                    # Connect to database
                    my $schema = eval { $schema_class->connect(@$connect_info); };
                    if ($@) {
                        warn "[PID $$] Database connection FAILED: $@"
                            if ASYNC_TRACE;
                        die "Failed to connect to database: $@";
                    }
                    unless (defined $schema) {
                        warn "[PID $$] Schema connection returned undef!"
                            if ASYNC_TRACE;
                        die "Schema connection returned undef";
                    }

                    warn "[PID $$] Database connected successfully"
                        if ASYNC_TRACE;

                    $schema_cache->{$pid} = $schema;

                    warn "[PID $$] Worker initialization complete"
                        if ASYNC_TRACE;
                }

                warn "[PID $$] STAGE 5 (Worker): Executing operation: $operation"
                    if ASYNC_TRACE;

                my $result = try {
                    my $schema = $schema_cache->{$pid};

                    warn "[PID $$] Schema from cache: " . (defined $schema ? ref($schema) : "UNDEF")
                        if ASYNC_TRACE;

                    if ($operation =~ /^(count|sum|max|min|avg|average)$/) {
                        warn "[PID $$] STAGE 6 (Worker): Performing aggregate $operation"
                            if ASYNC_TRACE;

                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond}  // {};
                        my $attrs       = $payload->{attrs} // {};
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
                            warn "[PID $$] WORKER ERROR: $@" if ASYNC_TRACE;
                            return { error => $@ };
                        } else {
                            # IMPORTANT: Force to scalar to avoid HASH(0x...) in Parent
                            # This stringifies potential Math::BigInt objects or references
                            warn "[PID $$] $operation complete: $val"
                                if ASYNC_TRACE;
                            return defined $val ? "$val" : undef;
                        }
                    }
                    elsif ($operation eq 'search' || $operation eq 'all') {
                        my $source_name = $payload->{source_name};
                        my $attrs       = $payload->{attrs} || {};

                        # Force collapse so DBIC merges the JOINed rows into nested objects
                        $attrs->{collapse} = 1 if $attrs->{prefetch};

                        my $rs = $schema->resultset($source_name)->search($payload->{cond}, $attrs);
                        my @rows = $rs->all;

                        # Use your proven old-design logic here
                        return [
                            map { _serialise_row_with_prefetch($_, $attrs->{prefetch}, {}) } @rows
                        ];
                    }
                    elsif ($operation eq 'update') {
                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond};
                        my $updates     = $payload->{updates};

                        if (!$updates || !keys %$updates) {
                            return 0;
                        } else {
                            return $schema->resultset($source_name)
                                          ->search($cond)
                                          ->update($updates);
                        }
                    }
                    elsif ($operation eq 'create') {
                        my $source_name = $payload->{source_name};
                        my $data        = $payload->{data};

                        # Perform the actual DBIC insert
                        my $row = $schema->resultset($source_name)->create($data);

                        # Sync with DB to get the Auto-Increment ID
                        # Some DBD drivers need this to populate the primary key in the object
                        $row->discard_changes;

                        my %raw = $row->get_columns;
                        my %clean_data;
                        for my $key (keys %raw) {
                            # Force stringification/numification to strip any DBIC internal "magic"
                            $clean_data{$key} = defined $raw{$key} ? "$raw{$key}" : undef;
                        }
                        return \%clean_data;
                    }
                    elsif ($operation eq 'delete') {
                        my $source_name = $payload->{source_name};
                        my $cond        = $payload->{cond};

                        # Direct delete on the resultset matching the condition
                        return $schema->resultset($source_name)->search($cond)->delete + 0;
                    }
                    elsif ($operation =~ /^populate(?:_bulk)?$/) {
                        my $source_name = $payload->{source_name};
                        my $data        = $payload->{data};

                        my $val = eval {
                            my $rs = $schema->resultset($source_name);

                            if ($operation eq 'populate') {
                                # Standard populate can return objects.
                                # We inflate them to HashRefs to pass back.
                                my @rows = $rs->populate($data);
                                return [ map { _serialise_row_with_prefetch($_, undef, {}) } @rows ];
                            }
                            else {
                                # populate_bulk is for speed; typically returns a count or truthy
                                $rs->populate($data); # DBIC void context usually
                                return 1;
                            }
                        };

                        if ($@) {
                            warn "[PID $$] WORKER ERROR: $@" if ASYNC_TRACE;
                            return { error => "$@" };
                        }
                        else {
                            return $val;
                        }
                    }
                    elsif ($operation eq 'find') {
                        my $source_name = $payload->{source_name};
                        my $query       = $payload->{query};
                        my $attrs       = $payload->{attrs} || {};

                        my $row = $schema->resultset($source_name)->find($query, $attrs);

                        if ($row) {
                            return _serialise_row_with_prefetch($row, undef, $attrs);
                        } else {
                            return;
                        }
                    }
                    elsif ($operation eq 'deploy') {
                        my ($sqlt_args, $dir) = (ref $payload eq 'ARRAY') ? @$payload : ($payload);
                        eval {
                            $schema->deploy($sqlt_args // {}, $dir);
                        };
                        if ($@) {
                            return { error => "Deploy operation failed: $@" };
                        }
                        else {
                            return { success => 1 };
                        }
                    }
                    elsif ($operation eq 'txn_batch') {
                        my $operations = $payload;

                        my $batch_result = eval {
                            $schema->txn_do(sub {
                                my $success_count = 0;
                                foreach my $op (@$operations) {
                                    my $type = $op->{type};
                                    my $rs_name = $op->{resultset};

                                    if ($type eq 'update') {
                                        my $row = $schema->resultset($rs_name)->find($op->{id});
                                        die "Record not found for update: $rs_name ID $op->{id}\n"
                                            unless $row;
                                        $row->update($op->{data});
                                        $success_count++;
                                    }
                                    elsif ($type eq 'create') {
                                        $schema->resultset($rs_name)->create($op->{data});
                                        $success_count++;
                                    }
                                    elsif ($type eq 'delete') {
                                        my $row = $schema->resultset($rs_name)->find($op->{id});
                                        die "Record not found for delete: $rs_name ID $op->{id}\n"
                                            unless $row;
                                        $row->delete;
                                        $success_count++;
                                    }
                                    elsif ($type eq 'raw') {
                                        $schema->storage->dbh->do($op->{sql}, undef, @{$op->{bind} || []});
                                        $success_count++;
                                    }
                                    else {
                                        die "Unknown operation type: $type\n";
                                    }
                                }
                                return { count => $success_count, success => 1 };
                            });
                        };

                        if ($@) {
                            return { error => "Batch Transaction Aborted: $@", success => 0 };
                        }
                        else {
                            return $batch_result;
                        }
                    }
                    elsif ($operation eq 'txn_do') {
                        my $steps = $payload;
                        my %register;

                        my $txn_result = eval {
                            $schema->txn_do(sub {
                                my @step_results;

                                foreach my $step (@$steps) {
                                    next unless $step && ref $step eq 'HASH'; # Skip empty/invalid steps
                                    next unless $step->{action};              # Skip steps with no action

                                    # 1. Resolve variables from previous steps
                                    # e.g., changing '$user_id' to the actual ID found in step 1
                                    _resolve_placeholders($step, \%register);

                                    # my $rs = $schema->resultset($step->{resultset});
                                    my $action = $step->{action};
                                    my $result_data;

                                    if ($action eq 'raw') {
                                        # Raw SQL bypasses the Resultset layer
                                        my $dbh = $schema->storage->dbh;
                                        $dbh->do($step->{sql}, undef, @{$step->{bind} || []});
                                        $result_data = { success => 1 };
                                    }
                                    else {
                                        # CRUD operations require a Resultset
                                        my $rs_name = $step->{resultset}
                                            or die "txn_do: action '$action' requires a 'resultset' parameter";
                                        my $rs = $schema->resultset($rs_name);

                                        if ($action eq 'create') {
                                            my $row = $rs->create($step->{data});
                                            $result_data = { id => $row->id, data => { $row->get_columns } };
                                        }
                                        elsif ($action eq 'find') {
                                            my $row = $rs->find($step->{id});
                                            die "txn_do: record not found" unless $row;
                                            $result_data = { id => $row->id, data => { $row->get_columns } };
                                        }
                                        elsif ($action eq 'update') {
                                            my $row = $rs->find($step->{id});
                                            die "txn_do: record not found for update" unless $row;
                                            $row->update($step->{data});
                                            $result_data = { success => 1, id => $row->id };
                                        }
                                    }

                                    if ($step->{name} && $result_data->{id}) {
                                        $register{ '$' . $step->{name} . '.id' } = $result_data->{id};
                                    }
                                    push @step_results, $result_data;
                                }
                                return { results => \@step_results, success => 1 };
                            });
                        };

                        return $@ ? { error => "Transaction failed: $@", success => 0 }
                                  : $txn_result;
                    }
                    elsif ($operation eq 'txn_begin') {
                        $schema->storage->txn_begin;
                        return { success => 1 };
                    }
                    elsif ($operation eq 'txn_commit') {
                        $schema->storage->txn_commit;
                        return { success => 1 };
                    }
                    elsif ($operation eq 'txn_rollback') {
                        $schema->storage->txn_rollback;
                        return { success => 1 };
                    }
                    elsif ($operation eq 'ping') {
                        return "pong";
                    }
                    else {
                        die "Unknown operation: $operation";
                    }
                }
                catch {
                    warn "[PID $$] Worker execution error: $_"
                        if ASYNC_TRACE;
                    return { error => "$_", success => 0 };
                };

                my $safe_result = $deflator->($result);
                if (ASYNC_TRACE) {
                    warn "[PID $$] Worker returning result type: " . ref($safe_result);
                    warn "[PID $$] Worker returning: $safe_result";
                }
                return $safe_result;
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

sub _resolve_placeholders {
    my ($item, $reg) = @_;
    return unless defined $item;

    if (ref $item eq 'HASH') {
        for my $key (keys %$item) {
            if (ref $item->{$key}) {
                # Dive deeper into nested structures
                _resolve_placeholders($item->{$key}, $reg);
            }
            elsif (defined $item->{$key} && exists $reg->{$item->{$key}}) {
                # Exact match: Swap '$user.id' for 42
                $item->{$key} = $reg->{$item->{$key}};
            }
            elsif (defined $item->{$key} && !ref $item->{$key}) {
                # String interpolation: Handle "ID is $user.id"
                $item->{$key} = _interpolate_string($item->{$key}, $reg);
            }
        }
    }
    elsif (ref $item eq 'ARRAY') {
        for my $i (0 .. $#$item) {
            if (ref $item->[$i]) {
                _resolve_placeholders($item->[$i], $reg);
            }
            elsif (defined $item->[$i] && exists $reg->{$item->[$i]}) {
                $item->[$i] = $reg->{$item->[$i]};
            }
        }
    }
}

sub _interpolate_string {
    my ($string, $reg) = @_;
    return $string unless $string =~ /\$/; # Optimization: skip if no $

    # Use a regex to find all keys in the register and replace them
    # Example: "INSERT INTO logs VALUES ('Created user $user.id')"
    foreach my $key (keys %$reg) {
        my $val = $reg->{$key};
        # Escape the key for regex safety (since it contains $)
        my $quoted_key = quotemeta($key);
        $string =~ s/$quoted_key/$val/g;
    }
    return $string;
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

sub _normalise_prefetch {
    my $pref = shift;
    return {} unless $pref;
    return { $pref => undef } unless ref $pref;
    if (ref $pref eq 'ARRAY') {
        return { map { %{ _normalise_prefetch($_) } } @$pref };
    }
    if (ref $pref eq 'HASH') {
        return $pref; # Already a spec
    }
    return {};
}

sub _serialise_row_with_prefetch {
    my ($row, $prefetch) = @_;
    return unless $row;

    # 1. Base columns
    my %data = $row->get_columns;

    # 2. Process Prefetches using the normalized spec
    if ($prefetch) {
        my $spec = _normalise_prefetch($prefetch);

        foreach my $rel (keys %$spec) {
            # Check if the row can actually perform this relationship
            if ($row->can($rel)) {
                # This is the "poke": calling the accessor $row->$rel
                # If prefetched, DBIC returns the data from memory.
                my $related = eval { $row->$rel };
                next if $@ || !defined $related;

                if (ref($related) eq 'DBIx::Class::ResultSet' || eval { $related->isa('DBIx::Class::ResultSet') }) {
                    # has_many: recurse into the collection
                    my @items = $related->all;
                    $data{$rel} = [
                        map { _serialise_row_with_prefetch($_, $spec->{$rel}) } @items
                    ];
                } elsif (eval { $related->isa('DBIx::Class::Row') }) {
                    # single: recurse into the row
                    $data{$rel} = _serialise_row_with_prefetch($related, $spec->{$rel});
                }
            }
        }
    }
    return \%data;
}

1; # End of DBIx::Class::Async
