package DBIx::Class::Async::ResultSet;

use strict;
use warnings;
use utf8;
use v5.14;

use Carp;
use Future;
use Scalar::Util 'blessed';
use DBIx::Class::Async;
use DBIx::Class::Async::Row;

use Data::Dumper;

sub new {
    my ($class, %args) = @_;

    # 1. Validation
    croak "Missing required argument: schema"      unless $args{schema};
    croak "Missing required argument: async_db"    unless $args{async_db};
    croak "Missing required argument: source_name" unless $args{source_name};

    # 2. Internal blessing
    return bless {
        _schema          => $args{schema},
        _schema_instance => $args{schema_instance},
        _async_db        => $args{async_db},
        _source_name     => $args{source_name},
        _result_class    => $args{result_class},
        _source          => undef,
        _cond            => $args{cond}  || {},
        _attrs           => $args{attrs} || {},
        _rows            => undef,
        _pos             => 0,
        _pager           => $args{pager} || undef,
        _entries         => $args{entries}       || undef,
        _is_prefetched   => $args{is_prefetched} || 0,
     }, $class;
}

sub new_result_set {
    my ($self, $args) = @_;

    my $class = ref $self;

    # Inherit from parent if not provided in args
    my $new_obj = {
        _async_db        => $args->{async_db}        // $self->{_async_db},
        _source_name     => $args->{source_name}     // $self->{_source_name},
        _schema          => $args->{schema}          // $self->{_schema},
        _schema_instance => $args->{schema_instance} // $self->{_schema_instance},
        _result_class    => $args->{result_class}    // $self->{_result_class},
        _cond            => $args->{cond}            // {},
        _attrs           => $args->{attrs}           // {},
        _rows            => undef,
        _pos             => $args->{pos}             // 0,
        _pager           => $args->{pager},
        _entries         => $args->{entries},
        _is_prefetched   => $args->{is_prefetched}   // 0,
    };

    return bless $new_obj, $class;
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

sub cursor {
    my $self = shift;

    return $self->{_schema_instance}->storage->cursor($self);
}

sub schema {
    my $self = shift;
    return $self->{_schema};
}

sub delete {
    my $self = shift;

    # If we have complex attributes (LIMIT, OFFSET, JOINs),
    # we MUST use the safe path.
    if ( keys %{$self->{_attrs} || {}} ) {
        return $self->delete_all;
    }

    # Path A: Simple, direct DELETE if condition is a clean HASH
    if ( ref($self->{_cond}) eq 'HASH' && keys %{$self->{_cond}} ) {
        return DBIx::Class::Async::delete(
            $self->{_async_db}, {
                source_name => $self->{_source_name},
                cond        => $self->{_cond}
            }
        );
    }

    # Default to the safe path for everything else
    return $self->delete_all;
}

sub delete_all {
    my $self = shift;

    # Step 1: Use our working 'all' method to get the targets
    return $self->all->then(sub {
        my $rows = shift;

        return Future->done(0) unless $rows && @$rows;

        # Step 2: Identify Primary Keys
        my @pks   = $self->result_source->primary_columns;
        my $count = scalar @$rows;
        my $condition;

        if (scalar @pks == 1) {
            my $pk_col = $pks[0];
            my @ids    = map { $_->get_column($pk_col) } @$rows;
            $condition = { $pk_col => { -in => \@ids } };
        }
        else {
            # Handle Composite Keys
            $condition = { -or => [
                map {
                    my $row = $_;
                    { map { $_ => $row->get_column($_) } @pks }
                } @$rows
            ]};
        }

        # Step 3: Send the targeted delete to the worker
        return DBIx::Class::Async::delete(
            $self->{_async_db}, {
                source_name => $self->{_source_name},
                cond        => $condition
            }
        )->then(sub {
            return Future->done($count);
        });
    });
}

sub create {
    my ($self, $data) = @_;

    # Merge conditions (Relationship context)
    my %to_insert = ( %{$self->{_cond} || {}}, %$data );

    # Clean prefixes (e.g., 'me.id' or 'foreign.user_id')
    my %final_data;
    while (my ($k, $v) = each %to_insert) {
        my $clean_key = $k;
        $clean_key =~ s/^(?:foreign|self|me)\.//;
        $final_data{$clean_key} = $v;
    }

    return DBIx::Class::Async::create(
        $self->{_async_db}, {
            source_name => $self->{_source_name},
            data => \%final_data
        }
    )->then(sub {
        my $db_data = shift;

        return Future->done($self->new_result($db_data, { in_storage => 1 }));
    });
}

sub update {
    my ($self, $data) = @_;

    # PATH A: Fast Path
    # Triggered if there are no complex attributes (like rows/offset),
    if ( keys %{$self->{_attrs} || {}} == 0 && $self->{_cond} && keys %{$self->{_cond}} ) {
        warn "[PID $$] update() - Taking Path A (Fast Path)";
        return DBIx::Class::Async::update(
            $self->{_async_db}, {
                source_name => $self->{_source_name},
                cond        => $self->{_cond},
                updates     => $data,
            }
        );
    }

    # PATH B: Safe Path
    # Use the ID-mapping strategy to respect LIMIT/OFFSET/Group By.
    warn "[PID $$] update() - Taking Path B (Safe Path via update_all)";
    return $self->update_all($data);
}

sub update_all {
    my ($self, $updates) = @_;
    my $bridge = $self->{_async_db};

    return $self->all->then(sub {
        my $rows = shift;

        # Hard check: is it really an arrayref?
        unless ($rows && ref($rows) eq 'ARRAY' && @$rows) {
            warn "[PID $$] update_all found no rows to update or invalid data type";
            return Future->done(0);
        }

        my ($pk) = $self->result_source->primary_columns;
        my @ids  = map { $_->get_column($pk) } @$rows;

         my $payload = {
            source_name => $self->{_source_name},
            cond        => { $pk => { -in => \@ids } },
            updates     => $updates,
        };

        return DBIx::Class::Async::update($bridge, $payload)->then(sub {
            my $affected = shift;
            return Future->done($affected);
        });
    });
}

sub update_or_new {
    my ($self, $data, $attrs) = @_;
    $attrs //= {};

    # Identify the primary key or unique constraint values for the lookup
    my $lookup = $self->_extract_unique_lookup($data, $attrs);

    return $self->find($lookup, $attrs)->then(sub {
        my ($row) = @_;

        if ($row) {
            # Object found in DB: trigger an async UPDATE
            return $row->update($data);
        }

        # Object NOT found: merge condition and data for a local 'new' object
        my %new_data = ( %{$self->{_cond} || {}}, %$data );
        my %clean_data;
        while (my ($k, $v) = each %new_data) {
            # Strip DBIC aliases so they don't crash the Row constructor
            (my $clean_key = $k) =~ s/^(?:me|foreign|self)\.//;
            $clean_data{$clean_key} = $v;
        }

        # Returns a Future wrapping the local Row object (in_storage = 0)
        return Future->done($self->new_result(\%clean_data));
    });
}

sub update_or_create {
    my ($self, $data, $attrs) = @_;
    $attrs //= {};

    my $lookup = $self->_extract_unique_lookup($data, $attrs);

    return $self->find($lookup, $attrs)->then(sub {
        my ($row) = @_;

        if ($row) {
            # 1. Standard Update Path
            return $row->update($data);
        }

        # 2. Not Found: Attempt Create
        return $self->create($data)->catch(sub {
            my ($error, $type) = @_;

            # If it's a DB unique constraint error, someone else beat us to the insert
            if ($type eq 'db_error' && "$error" =~ /unique constraint|already exists/i) {

                # 3. Race Recovery: Re-find the winner and update them
                return $self->find($lookup, $attrs)->then(sub {
                    my ($recovered) = @_;
                    return $recovered
                        ? $recovered->update($data)
                        : Future->fail("Race recovery failed: record vanished after conflict", "logic_error");
                });
            }

            # Otherwise, bubble up the original error
            return Future->fail($error, $type);
        });
    });
}

sub page {
    my ($self, $page_number) = @_;

    # 1. Ensure we have a valid page number (default to 1)
    my $page = $page_number || 1;

    # 2. Capture existing rows attribute from _attrs, or default to 10
    # This matches your old design's requirement
    my $rows = $self->{_attrs}->{rows} || 10;

    # 3. Delegate to search() for cloning and state preservation
    # This passes through your bridge validation logic
    return $self->search(undef, {
        page => $page,
        rows => $rows,
    });
}

sub pager {
    my $self = shift;

    # 1. Return cached pager if it exists
    return $self->{_pager} if $self->{_pager};

    # 2. Strict check for paging attributes
    unless ($self->is_paged) {
        die "Cannot call ->pager on a non-paged resultset. Call ->page(\$n) first.";
    }

    # 3. Warning for unordered results (crucial for consistent pagination)
    # Checks if we are NOT in a test environment (HARNESS_ACTIVE)
    if (!$self->is_ordered && !$ENV{HARNESS_ACTIVE}) {
        warn "DBIx::Class::Async Warning: Calling ->pager on an unordered ResultSet. " .
             "Results may be inconsistent across pages.\n";
    }

    # 4. Lazy-load and instantiate the Async Pager
    require DBIx::Class::Async::ResultSet::Pager;
    return $self->{_pager} = DBIx::Class::Async::ResultSet::Pager->new(resultset => $self);
}

sub is_ordered {
    my $self = shift;
    # Check if 'order_by' exists in the attributes hashref
    return (exists $self->{_attrs}->{order_by} && defined $self->{_attrs}->{order_by}) ? 1 : 0;
}

sub is_paged {
    my $self = shift;
    return (exists $self->{_attrs}->{page} && defined $self->{_attrs}->{page}) ? 1 : 0;
}

sub _extract_unique_lookup {
    my ($self, $data, $attrs) = @_;

    my $source = $self->result_source;
    my $key_name = $attrs->{key} || 'primary';
    my @unique_cols = $source->unique_constraint_columns($key_name);

    # Alias-aware grep for primary check
    if (!grep { exists $data->{$_} || exists $data->{"me.$_"} } @unique_cols) {
        foreach my $constraint ($source->unique_constraint_names) {
            my @cols = $source->unique_constraint_columns($constraint);
            # Alias-aware grep for discovery loop
            if (grep { exists $data->{$_} || exists $data->{"me.$_"} } @cols) {
                @unique_cols = @cols;
                last;
            }
        }
    }

    # Build the lookup, checking for aliases
    my %lookup;
    foreach my $col (@unique_cols) {
        if (exists $data->{$col}) {
            $lookup{$col} = $data->{$col};
        }
        elsif (exists $data->{"me.$col"}) {
            $lookup{$col} = $data->{"me.$col"};
        }
    }

    # Absolute fallback
    return keys %lookup ? \%lookup : $data;
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

sub count_future {
    my $self = shift;
    my $db   = $self->{_async_db};

    return DBIx::Class::Async::count($db, {
        source_name => $self->{_source_name},
        cond        => $self->{_cond},
        attrs       => $self->{_attrs},
    });
}

sub count_total {
    my ($self, $cond, $attrs) = @_;

    # 1. Merge incoming parameters with existing ResultSet state
    my %merged_cond  = ( %{ $self->{_cond}  || {} }, %{ $cond  || {} } );
    my %merged_attrs = ( %{ $self->{_attrs} || {} }, %{ $attrs || {} } );

    # 2. Strip slicing/ordering attributes to get the absolute total
    delete @merged_attrs{qw(rows offset page order_by)};

    # 3. Use the static call exactly like your other count() implementations
    # $self->{async_db} is the $db handle passed as the first arg
    return DBIx::Class::Async::count($self->{_async_db}, {
        source_name => $self->{_source_name},
        cond        => \%merged_cond,
        attrs       => \%merged_attrs,
    });
}

sub all {
    my ($self) = @_;

    # 1. Return cached objects if we already have them
    if ($self->{_rows} && ref($self->{_rows}) eq 'ARRAY') {
        return Future->done($self->{_rows});
    }

    # 2. Handle Prefetched/Manual entries (The "Pass-through" logic)
    if ($self->{_is_prefetched} && $self->{_entries}) {
        $self->{_rows} = [
            map {
                (ref($_) && $_->isa('DBIx::Class::Async::Row'))
                ? $_
                : $self->new_result($_, { in_storage => 1 })
            } @{$self->{_entries}}
        ];
        return Future->done($self->{_rows});
    }

    # 3. Standard Async Fetch (The Bridge + Inflation)
    # Increment stats here since we are actually hitting the wire
    $self->{_async_db}{_stats}{_queries}++;
    warn "[PID $$] STAGE 1 (Parent): Dispatching all() via all_future";

    return $self->all_future->then(sub {
        my ($rows) = @_;

        $self->{_rows} = $rows; # Cache the Hijacked Objects
        $self->{_pos}  = 0;     # Reset iterator position

        return Future->done($rows);
    });
}

sub all_future {
    my $self = shift;
    my $db   = $self->{_async_db};

    return DBIx::Class::Async::all($db, {
        source_name => $self->{_source_name},
        cond        => $self->{_cond},
        attrs       => $self->{_attrs},
    })->then(sub {
        my $rows_data = shift;

        if (!ref($rows_data) || ref($rows_data) ne 'ARRAY') {
            warn "[PID $$] Bridge error: expected ARRAYREF, got: " . ($rows_data // 'undef');
            return Future->done([]); # Return empty to avoid crash
        }

        my @objects   = map { $self->new_result($_, { in_storage => 1 }) } @$rows_data;
        return Future->done(\@objects);
    });
}

sub new_result {
    my ($self, $data, $attrs) = @_;
    return undef unless defined $data;

    my $storage_hint = (ref $attrs eq 'HASH') ? $attrs->{in_storage} : undef;
    my $db = $self->{_async_db};
    my $result_source = $self->result_source;

    # 1. Row Construction
    my $row = DBIx::Class::Async::Row->new(
        schema        => $self->{_schema},   # The actual schema object
        async_db      => $db,                # The plain hashref itself
        source_name   => $self->{_source_name},
        result_source => $result_source,
        row_data      => $data,
        in_storage    => $storage_hint // 0,
    );

    # 2. Dynamic Class Hijacking
    my $target_class   = $self->{_result_class} || $self->result_source->result_class;
    my $base_row_class = ref($row);

    if ($target_class ne $base_row_class) {
        my $anon_class = "DBIx::Class::Async::Anon::" . ($base_row_class . "_" . $target_class) =~ s/::/_/gr;

        no strict 'refs';
        unless (@{"${anon_class}::ISA"}) {
            # Ensure the target result class is loaded
            eval "require $target_class" unless $target_class->can('new');
            @{"${anon_class}::ISA"} = ($base_row_class, $target_class);
        }
        bless $row, $anon_class;
    }

    return $row;
}

sub result_source {
    my $self = shift;
    return $self->_get_source;
}

sub _get_source {
    my $self = shift;
    $self->{_source} ||= $self->{_schema}->source($self->{_source_name});
    return $self->{_source};
}

sub search {
    my ($self, $cond, $attrs) = @_;

    my $new_cond;

    # If the new condition is a literal (Scalar/Ref), it overrides everything
    if (ref $cond eq 'REF' || ref $cond eq 'SCALAR') {
        $new_cond = $cond;
    }
    # If the new condition is a Hash, merge it with existing conditions
    elsif (ref $cond eq 'HASH') {
        if (ref $self->{_cond} eq 'HASH' && keys %{$self->{_cond}}) {
            # Use -and to combine the current state with the new criteria
            $new_cond = { -and => [ $self->{_cond}, $cond ] };
        }
        else {
            # If current condition is empty, just use the new one
            $new_cond = $cond;
        }
    }
    else {
        # Fallback for simple cases (like passing undef)
        $new_cond = $cond || $self->{_cond};
    }

    my $merged_attrs = { %{$self->{_attrs} || {}}, %{$attrs || {}} };

    # We return a clone of the current object but with the updated "State"
    return $self->new_result_set({
        source_name   => $self->{_source_name},
        cond          => $new_cond,
        attrs         => $merged_attrs,
        result_class  => $attrs->{result_class} // $self->{_result_class},
        pos           => 0,
        pager         => undef,
        entries       => undef,
        is_prefetched => 0,
    });
}

sub search_rs {
    my $self = shift;
    return $self->search(@_);
}

sub search_related {
    my ($self, $rel_name, $cond, $attrs) = @_;

    # Use the helper to get the new configuration
    my $new_rs = $self->search_related_rs($rel_name, $cond, $attrs);

    # In an async context, search_related usually implies
    # wanting the ResultSet object to call ->all_future on later.
    return $new_rs;
}

sub search_related_rs {
    my ($self, $rel_name, $cond, $attrs) = @_;

    # 1. Get the source. If _source is undef, pull it from the schema
    my $source = $self->{_source}
              || $self->{_schema}->resultset($self->{_source_name})->result_source;

    # 2. Create the Shadow RS
    require DBIx::Class::ResultSet;
    my $shadow_rs = DBIx::Class::ResultSet->new($source, {
        cond  => $self->can('ident_condition') ? { $self->ident_condition } : $self->{_cond},
        attrs => $self->{_attrs} || {},
    });

    # 3. Pivot
    my $related_shadow = $shadow_rs->search_related($rel_name, $cond, $attrs);

    # 4. Wrap with ALL required keys for your constructor
    return DBIx::Class::Async::ResultSet->new(
        schema      => $self->{_schema},
        async_db    => $self->{_async_db},
        source_name => $related_shadow->result_source->source_name,
        cond        => $related_shadow->{cond},
        attrs       => $related_shadow->{attrs},
    );
}

sub find {
    my ($self, $id_or_cond) = @_;

    return Future->done(undef) unless defined $id_or_cond;

    # If it's a HASH, use it directly.
    # If it's a scalar, assume it's the Primary Key 'id'.
    my $cond = ref($id_or_cond) eq 'HASH' ? $id_or_cond : { id => $id_or_cond };

    warn "[PID $$] find() searching with: " . (ref $cond ? "HASH" : $cond);

    my $rs = $self->search_rs($cond);
    return $rs->single;
}

sub single {
    my $self = shift;
    warn "[PID $$] single() called, _rows=" . (defined $self->{_rows} ? ref($self->{_rows}) || 'SCALAR:' . $self->{_rows} : 'undef');

    # We use search here to ensure we only ask the worker for 1 row
    my $rs = $self->search(undef, { rows => 1 });
    warn "[PID $$] single() created search with rows=1, _rows=" . (defined $rs->{_rows} ? ref($rs->{_rows}) || 'SCALAR:' . $rs->{_rows} : 'undef');

    return $rs->next;
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

sub next {
    my $self = shift;

    # 1. Check if the buffer already exists
    if ($self->{_rows}) {
        $self->{_pos} //= 0;

        # End of buffer reached
        if ($self->{_pos} >= @{$self->{_rows}}) {
            return Future->done(undef);
        }

        my $data = $self->{_rows}[$self->{_pos}++];

        # Inflate if it's raw data, otherwise return as is
        my $row = (ref($data) eq 'HASH')
            ? $self->new_result($data, { in_storage => 1 })
            : $data;

        return Future->done($row);
    }

    # 2. Buffer empty: Trigger 'all'
    return $self->all->then(sub {
        my $rows = shift;

        if (!$rows || !@$rows) {
            return Future->done(undef);
        }

        $self->{_pos} //= 0;

        return $self->{_rows}[$self->{_pos}++];
    });
}

1; # End of DBIx::Class::Async::ResultSet
