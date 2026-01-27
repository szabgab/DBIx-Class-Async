package DBIx::Class::Async::ResultSet;

use strict;
use warnings;
use utf8;
use v5.14;

use Carp;
use Future;
use Data::Dumper;
use Scalar::Util 'blessed';
use DBIx::Class::Async;
use DBIx::Class::Async::Row;
use DBIx::Class::Async::ResultSetColumn;

sub new {
    my ($class, %args) = @_;

    # 1. Validation
    croak "Missing required argument: schema_instance" unless $args{schema_instance};
    croak "Missing required argument: source_name"     unless $args{source_name};

    # 2. Blessing the unified state
    return bless {
        # Core Infrastructure
        _schema_instance => $args{schema_instance},
        _async_db        => $args{async_db} // $args{_schema_instance}->{_async_db},

        # Source Metadata
        _source_name     => $args{source_name},
        _result_class    => $args{result_class} // 'DBIx::Class::Core',

        # Query State
        _cond            => $args{cond}  // {},
        _attrs           => $args{attrs} // {},

        # Result State (Usually reset on clone)
        _rows            => $args{rows} // undef,
        _pos             => $args{pos}  // 0,

        # Prefetch/Pager logic
        _is_prefetched   => $args{is_prefetched} // 0,
        _pager           => $args{pager},
    }, $class;
}

sub new_result {
    my ($self, $data, $attrs) = @_;
    return undef unless defined $data;

    my $storage_hint = (ref $attrs eq 'HASH') ? $attrs->{in_storage} : undef;

    # Use the standardized internal keys
    my $db = $self->{_async_db};
    my $schema_instance = $self->{_schema_instance};

    # 1. Row Construction
    # Note: We pass clean keys (no underscores) to match your Row constructor rule
    my $row = DBIx::Class::Async::Row->new(
        schema_instance => $schema_instance,
        async_db        => $db,
        source_name     => $self->{_source_name},
        row_data        => $data,
        in_storage      => $storage_hint // 0,
    );

    # 2. Dynamic Class Hijacking (Preserved logic)
    my $target_class   = $self->{_result_class};
    my $base_row_class = ref($row);

    if ($target_class && $target_class ne $base_row_class) {
        # Create a unique name for the hybrid class
        my $anon_class = "DBIx::Class::Async::Anon::" . ($base_row_class . "_" . $target_class) =~ s/::/_/gr;

        no strict 'refs';
        unless (@{"${anon_class}::ISA"}) {
            eval "require $target_class" unless $target_class->can('new');
            # Multi-inheritance: Async capabilities + Result class methods
            @{"${anon_class}::ISA"} = ($base_row_class, $target_class);
        }
        bless $row, $anon_class;

        # Re-run accessor installation for the new hijacked class
        $row->_ensure_accessors;
    }

    return $row;
}

sub new_result_set {
    my ($self, $overrides) = @_;

    my %args;
    foreach my $internal_key (keys %$self) {
        # 1. Only process keys starting with an underscore
        next unless $internal_key =~ /^_/;

        # 2. Strip leading underscore for the "clean" argument name
        my $clean_key = $internal_key;
        $clean_key =~ s/^_//;

        $args{$clean_key} = $self->{$internal_key};
    }

    $args{async_db}        = $self->{_async_db};
    $args{schema_instance} = $self->{_schema_instance};

    # 3. Apply overrides
    if ($overrides) {
        @args{keys %$overrides} = values %$overrides;
    }

    # 4. Call new() with a flat list (%args)
    return (ref $self)->new(%args);
}

############################################################################

sub all {
    my ($self) = @_;

    # 1. Return cached objects if we already have them
    if ($self->{_rows} && ref($self->{_rows}) eq 'ARRAY') {
        return Future->done($self->{_rows});
    }

    # 2. Handle Prefetched/Manual entries
    if ($self->{_is_prefetched} && $self->{_entries}) {
        $self->{_rows} = [
            map {
                (blessed($_) && $_->isa('DBIx::Class::Row'))
                ? $_
                : $self->_inflate_row($_)
            } @{$self->{_entries}}
        ];
        return Future->done($self->{_rows});
    }

    # 3. Standard Async Fetch
    return $self->all_future->then(sub {
        my ($rows) = @_;
        $self->{_rows} = $rows;
        $self->{_pos}  = 0;
        return Future->done($rows);
    });
}

sub all_future {
    my $self = shift;

    my $db      = $self->{_async_db};
    my $payload = $self->_build_payload();
    $payload->{source_name} = $self->{_source_name};
    $payload->{cond}        = $self->{_cond};
    $payload->{attrs}       = $self->{_attrs};

    return DBIx::Class::Async::_call_worker(
        $db,
        'search',
        $payload,
    )->then(sub {
        my $rows_data = shift;

        if (!ref($rows_data) || ref($rows_data) ne 'ARRAY') {
            return Future->done([]);
        }

        # Use the helper to ensure nested relations are inflated
        my @objects = map { $self->_inflate_row($_) } @$rows_data;

        return Future->done(\@objects);
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

############################################################################

sub cursor {
    my $self = shift;

    return $self->{_schema_instance}->storage->cursor($self);
}

sub create {
    my ($self, $raw_data) = @_;
    my $db          = $self->{_async_db};
    my $source_name = $self->{_source_name};

    # 1. Fetch inflators
    my $inflators = $db->{_custom_inflators}{$source_name} || {};

    # 2. Deflate the incoming data (Parent Side)
    my %deflated_data;
    while (my ($k, $v) = each %$raw_data) {
        my $clean_key = $k;
        $clean_key =~ s/^(?:foreign|self|me)\.//;

        if ($inflators->{$clean_key} && $inflators->{$clean_key}{deflate}) {
            $v = $inflators->{$clean_key}{deflate}->($v);
        }
        $deflated_data{$clean_key} = $v;
    }

    # 3. Leverage your specialized payload builder
    # We pass the deflated data as the 'cond' or 'data'
    # depending on how your worker expects 'create' to look.
    my $payload = $self->_build_payload(\%deflated_data);

    # Ensure the worker sees this as the 'data' key for insertion
    $payload->{data} = \%deflated_data;

    # 4. Dispatch with correct signature
    return DBIx::Class::Async::_call_worker(
        $db,
        'create',
        $payload
    )->then(sub {
        my $db_row = shift;
        return Future->done(undef) unless $db_row;

        # 5. Inflation of return data
        for my $col (keys %$inflators) {
            if (exists $db_row->{$col} && $inflators->{$col}{inflate}) {
                $db_row->{$col} = $inflators->{$col}{inflate}->($db_row->{$col});
            }
        }

        # 6. Hydrate into an Async-aware Row
        my $obj = $self->result_source->result_class->new({});
        $obj->{_data}          = { %$db_row };
        $obj->{_in_storage}    = 1;
        $obj->{_dirty}         = {};
        $obj->{_source_name}   = $source_name;
        $obj->{_result_source} = $self->result_source;
        $obj->{_async_db}      = $db;

        bless $obj, 'DBIx::Class::Async::Row';
        return Future->done($obj);
    });
}

sub count {
    my ($self, $cond, $attrs) = @_;

    my $db = $self->{_async_db};

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

sub count_literal {
    my ($self, $sql_fragment, @bind) = @_;

    # 1. search_literal() creates a NEW ResultSet instance.
    # 2. Because we fixed search_literal/new_result_set, this new RS
    #    already shares the same _async_db and _schema_instance.
    # 3. We then chain the count() call which returns the Future.

    return $self->search_literal($sql_fragment, @bind)->count;
}

sub count_rs {
    my ($self, $cond, $attrs) = @_;

    # By calling $self->search, we guarantee the new RS
    # inherits the pinned _async_db and _schema_instance.
    return $self->search($cond, {
        %{ $attrs || {} },
        select => [ { count => '*' } ],
        as     => [ 'count' ],
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
    return DBIx::Class::Async::count($self->{_async_db}, {
        source_name => $self->{_source_name},
        cond        => \%merged_cond,
        attrs       => \%merged_attrs,
    });
}

############################################################################

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

############################################################################

sub first_future  { shift->first(@_) }

sub first {
    my $self = shift;

    # 1. If we already have data in memory, use it!
    if ($self->{_rows} && @{$self->{_rows}}) {
        my $data = $self->{_rows}[0];
        my $row = (ref($data) eq 'HASH') ? $self->_inflate_row($data) : $data;
        return Future->done($row);
    }

    # 2. If no cache, force a LIMIT 1 query to be fast
    return $self->search(undef, { rows => 1 })->next;
}

sub find {
    my ($self, $id_or_cond) = @_;

    # 1. Immediate return for undef
    return Future->done(undef) unless defined $id_or_cond;

    # 2. Build condition
    my $cond = ref($id_or_cond) eq 'HASH' ? $id_or_cond : { id => $id_or_cond };

    # 3. Create a new ResultSet state with the condition
    # search() uses our generic new_result_set() translator
    my $rs = $self->search($cond);

    # 4. Delegate execution to single()
    return $rs->single;
}

sub find_or_new {
    my ($self, $data, $attrs) = @_;
    $attrs //= {};

    # 1. Identify what makes this record unique
    my $lookup = $self->_extract_unique_lookup($data, $attrs);

    # 2. Call our newly ported find()
    return $self->find($lookup, $attrs)->then(sub {
        my ($row) = @_;

        # If found, return it immediately
        return Future->done($row) if $row;

        # 3. Otherwise, prepare data for a new local object
        # We merge existing constraints with the provided data
        my %new_data = ( %{$self->{_cond} || {}}, %$data );
        my %clean_data;
        while (my ($k, $v) = each %new_data) {
            (my $clean_key = $k) =~ s/^(?:me|foreign|self)\.//;
            $clean_data{$clean_key} = $v;
        }

        # 4. Return a "new" result object (local memory only)
        # Note: new_result should handle passing the _async_db to the row
        return Future->done($self->new_result(\%clean_data));
    });
}

sub find_or_create {
    my ($self, $data, $attrs) = @_;
    $attrs //= {};

    my $lookup = $self->_extract_unique_lookup($data, $attrs);

    # 1. First attempt: Find
    return $self->find($lookup, $attrs)->then(sub {
        my ($row) = @_;
        return Future->done($row) if $row;

        # 2. Second attempt: Create
        # This calls your async create() which goes through the bridge
        return $self->create($data)->catch(sub {
            my ($error) = @_;

            # 3. Race Condition Recovery
            # If the error is about a unique constraint, someone else inserted it
            # between our 'find' and 'create' calls.
            if ("$error" =~ /unique constraint|already exists/i) {
                warn "[PID $$] Race condition detected in find_or_create, retrying find";
                return $self->find($lookup, $attrs);
            }

            # If it's a real error (connection, etc.), fail forward
            return Future->fail($error);
        });
    });
}

############################################################################

sub get {
    my $self = shift;
    # 1. Check for inflated objects first
    return $self->{_rows} if $self->{_rows} && ref($self->{_rows}) eq 'ARRAY';

    # 2. Check for raw data awaiting inflation
    return $self->{_entries} if $self->{_entries} && ref($self->{_entries}) eq 'ARRAY';

    return [];
}

sub get_cache {
    my $self = shift;
    # Align with your all() logic: Return _rows if populated, otherwise undef
    return $self->{_rows} if $self->{_rows} && ref($self->{_rows}) eq 'ARRAY';

    # Optional: If you want get_cache to be "smart" like your line 85,
    # you could return _entries here, but usually get_cache implies inflated rows.
    return undef;
}

sub get_column {
    my ($self, $column) = @_;

    return DBIx::Class::Async::ResultSetColumn->new(
        resultset => $self,
        column    => $column,
        async_db  => $self->{_async_db},
    );
}

############################################################################

sub is_ordered {
    my $self = shift;
    # Check if 'order_by' exists in the attributes hashref
    return (exists $self->{_attrs}->{order_by} && defined $self->{_attrs}->{order_by}) ? 1 : 0;
}

sub is_paged {
    my $self = shift;
    return (exists $self->{_attrs}->{page} && defined $self->{_attrs}->{page}) ? 1 : 0;
}

############################################################################

sub next {
    my $self = shift;

    # 1. Check if the buffer already exists (Memory Hit)
    if ($self->{_rows}) {
        $self->{_pos} //= 0;

        if ($self->{_pos} >= @{$self->{_rows}}) {
            return Future->done(undef);
        }

        my $data = $self->{_rows}[$self->{_pos}++];

        # Inflate if it's raw data
        my $row = (ref($data) eq 'HASH')
            ? $self->_inflate_row($data)
            : $data;

        return Future->done($row);
    }

    # 2. Buffer empty: Trigger 'all' (Database Hit)
    return $self->all->then(sub {
        # $self->all already inflated the rows into $self->{_rows}
        # and returns an arrayref of objects.
        my $rows = shift;

        if (!$rows || !@$rows) {
            return Future->done(undef);
        }

        # Reset position and return the first inflated result
        $self->{_rows} = $rows;
        $self->{_pos}  = 0;
        my $row = $self->{_rows}[$self->{_pos}++];

        $row = $self->_inflate_row($row) if ref($row) eq 'HASH';

        return Future->done($row);
    });
}

############################################################################

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

sub populate {
    my ($self, $data) = @_;
    return $self->_do_populate('populate', $data);
}

sub populate_bulk {
    my ($self, $data) = @_;
    return $self->_do_populate('populate_bulk', $data);
}

sub _do_populate {
    my ($self, $operation, $data) = @_;

    croak("data required") unless defined $data;
    croak("data must be an arrayref") unless ref $data eq 'ARRAY';
    return Future->done([]) unless @$data;

    # 1. Build payload and STRICTLY validate
    my $payload = $self->_build_payload();

    croak("Failed to build payload: _build_payload returned undef")
        unless ref $payload eq 'HASH';

    croak("Missing source_name in ResultSet")
        unless $payload->{source_name} || $self->{_source_name};

    # 2. Deflate the data for the Worker
    my $db          = $self->{_async_db};
    my $source_name = $self->{_source_name};
    my $inflators   = $db->{_custom_inflators}{$source_name} || {};

    # 1. Deflate the data for the Worker
    my @deflated_data;

    # Check if this is the "Array of Arrays" format (first element is an arrayref)
    if (ref $data->[0] eq 'ARRAY') {
        # This is the header-style populate: [['col1', 'col2'], [val1, val2]]
        # We pass it through raw, as the Worker should handle the mapping,
        # but we still want to keep our deflation logic if possible.
        @deflated_data = @$data;
    }
    else {
        # This is the "Array of Hashes" format
        foreach my $row_data (@$data) {
            croak("populate row must be a HASH ref") unless ref $row_data eq 'HASH';

            my %deflated_row;
            while (my ($k, $v) = each %$row_data) {
                my $clean_key = $k;
                $clean_key =~ s/^(?:foreign|self|me)\.//;

                if ($inflators->{$clean_key} && $inflators->{$clean_key}{deflate}) {
                    $v = $inflators->{$clean_key}{deflate}->($v);
                }
                $deflated_row{$clean_key} = $v;
            }
            push @deflated_data, \%deflated_row;
        }
    }

    # 3. Patch and Dispatch
    $payload->{source_name} //= $source_name;
    $payload->{data}          = \@deflated_data;

    return DBIx::Class::Async::_call_worker(
        $db,
        $operation,
        $payload
    )->then(sub {
        my $results = shift;
        return Future->done([]) unless $results && ref $results eq 'ARRAY';

        my @objects = map { $self->_inflate_row($_) } @$results;
        return Future->done(\@objects);
    });
}

sub prefetch {
    my ($self, $prefetch) = @_;
    return $self->search(undef, { prefetch => $prefetch });
}

############################################################################

sub result_class {
    my $self = shift;

    if (@_) {
        # Clone check: In DBIC, changing attributes usually returns a new RS
        # but if you're modifying in place:
        $self->{_attrs}->{result_class} = shift;
        return $self;
    }

    # Resolve hierarchy:
    # 1. Look in the Async attributes (_attrs)
    # 2. Fall back to the ResultSource default
    return $self->{_attrs}->{result_class}
        || $self->result_source->result_class;
}

sub related_resultset {
    my ($self, $rel_name) = @_;

    # 1. Get current source and schema link
    my $source        = $self->result_source;
    my $schema_inst   = $self->{_schema_instance};
    my $native_schema = $schema_inst->{_native_schema};

    # 2. Resolve relationship info
    my $rel_info = $source->relationship_info($rel_name)
        or die "No such relationship '$rel_name' on " . $source->source_name;

    # 3. Determine the Target Moniker
    # DBIC often returns full class names (TestSchema::Result::Order)
    # but ->source() and ->resultset() want the moniker (Order)
    my $target_moniker = $rel_info->{source};
    $target_moniker =~ s/^.*::Result:://;

    # 4. Resolve the target source object (for metadata/pivoting)
    my $rel_source_obj = eval { $native_schema->source($target_moniker) }
        || eval { $native_schema->source($rel_info->{source}) };

    unless ($rel_source_obj) {
        die "Could not resolve source for relationship '$rel_name' (target: $target_moniker)";
    }

    # 5. Find the reverse relationship (e.g., 'user') for the JOIN
    my $reverse_rel = $self->_find_reverse_relationship($source, $rel_source_obj, $rel_name)
        or die "Could not find reverse relationship for '$rel_name' to " . $source->source_name;

    # 6. Prefix existing conditions (Pivot logic)
    # Turns { age => 30 } into { 'user.age' => 30 }
    my %new_cond;
    if ($self->{_cond} && ref $self->{_cond} eq 'HASH') {
        while (my ($key, $val) = each %{$self->{_cond}}) {
            my $new_key = ($key =~ /\./) ? $key : "$reverse_rel.$key";
            $new_cond{$new_key} = $val;
        }
    }

    # 7. Build the new Async ResultSet using the MONIKER
    # We call resultset('Order'), NOT resultset('orders')
    return $schema_inst->resultset($target_moniker)->search(
        \%new_cond,
        { join => $reverse_rel }
    );
}

sub result_source {
    my $self = shift;
    return $self->_get_source;
}

sub _get_source {
    my $self = shift;

    $self->{_source} ||= $self->{_schema_instance}->source($self->{_source_name});

    return $self->{_source};
}

sub reset_stats {
    my $self = shift;
    foreach my $key (keys %{ $self->{_async_db}->{_stats} }) {
        $self->{_async_db}->{_stats}->{$key} = 0;
    }
    return $self;
}

sub reset {
    my $self = shift;
    $self->{_pos} = 0;
    return $self;
}

############################################################################

# alias for result_source()

sub source {
    my $self = shift;
    return $self->_get_source;
}

sub source_name {
    my $self = shift;
    return $self->{_source_name};
}

sub search {
    my ($self, $cond, $attrs) = @_;

    my $new_cond;

    if (ref $cond eq 'REF' || ref $cond eq 'SCALAR') {
        $new_cond = $cond;
    }
    elsif (ref $cond eq 'HASH') {
        if (ref $self->{_cond} eq 'HASH' && keys %{$self->{_cond}}) {
            $new_cond = { -and => [ $self->{_cond}, $cond ] };
        }
        else {
            $new_cond = $cond;
        }
    }
    else {
        $new_cond = $cond || $self->{_cond};
    }

    my $merged_attrs = { %{$self->{_attrs} || {}}, %{$attrs || {}} };

    return $self->new_result_set({
        cond            => $new_cond,
        attrs           => $merged_attrs,
        #async_db        => $self->{_async_db},
        #schema_instance => $self->{_schema_instance},
        pos           => 0,
        pager         => undef,
        entries       => undef,
        is_prefetched => 0,
    });
}

sub search_literal {
    my ($self, $sql_fragment, @bind) = @_;

    # By passing it to $self->search, we guarantee:
    # 1. The new ResultSet gets the pinned _async_db and _schema_instance.
    # 2. Any existing 'where' or 'attrs' (like rows/order_by) are merged.
    return $self->search(
        \[ $sql_fragment, @bind ]
    );
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
    my $source = $self->{_result_source}
              || $self->{_schema_instance}->resultset($self->{_source_name})->result_source;

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
        schema_instance => $self->{_schema_instance},
        async_db    => $self->{_async_db},
        source_name => $related_shadow->result_source->source_name,
        cond        => $related_shadow->{cond},
        attrs       => $related_shadow->{attrs},
    );
}

sub search_with_pager {
    my ($self, $cond, $attrs) = @_;

    # 1. Create the paged resultset
    # This applies any search conditions and returns a new RS instance
    my $paged_rs = $self->search($cond, $attrs);

    # 2. Ensure paging is actually active
    # If the user didn't provide 'rows' or 'page' in $attrs, we force page 1
    if (!$paged_rs->is_paged) {
        $paged_rs = $paged_rs->page(1);
    }

    # 3. Instantiate the Async Pager (using your existing method)
    my $pager = $paged_rs->pager;

    # 4. Fire parallel requests to the worker pool
    # 'all' initiates the data fetch; 'total_entries' initiates the count(*)
    my $data_f  = $paged_rs->all;
    my $total_f = $pager->total_entries;

    # 5. Return a combined Future
    # This resolves only when BOTH the data and the count are back from workers
    return Future->needs_all($data_f, $total_f)->then(sub {
        my ($rows, $total) = @_;

        # At this point, $pager->total_entries is already resolved internally
        # so the user can immediately call $pager->last_page, etc.
        return Future->done($rows, $pager);
    });
}

sub single        { shift->first }

sub single_future { shift->first }

sub stats {
    my ($self, $key) = @_;

    # Return the whole stats hash if no key is provided
    return $self->{_async_db}->{_stats} unless $key;

    # Otherwise return the specific metric (e.g., 'queries')
    # Note: We map the public 'queries' to the internal '_queries'
    my $internal_key = $key =~ /^_/ ? $key : "_$key";
    return $self->{_async_db}->{_stats}->{$internal_key};
}

sub schema {
    my $self = shift;
    return $self->{_schema};
}

sub slice {
    my ($self, $first, $last) = @_;
    require Carp;

    # 1. Validation logic (remains the same)
    Carp::croak("slice requires two arguments (first and last index)")
        unless defined $first && defined $last;
    Carp::croak("slice indices must be non-negative integers")
        if $first < 0 || $last < 0;
    Carp::croak("first index must be less than or equal to last index")
        if $first > $last;

    # 2. Calculate pagination parameters
    my $offset = $first;
    my $rows   = $last - $first + 1;

    # 3. Create the limited ResultSet
    # Since search() already handles cloning and attr merging, use it!
    my $sliced_rs = $self->search(undef, {
        offset => $offset,
        rows   => $rows,
    });

    # 4. Context-aware return
    if (!wantarray) {
        # Scalar context: Return the RS for further chaining
        return $sliced_rs;
    }

    # List context: This is tricky in Async.
    # In standard DBIC, slice() in list context executes immediately.
    # To keep your current test style, we'll return the results of 'all'.
    # Note: If your 'all' returns a Future, list context users must
    # be aware they are getting a single Future object, not the rows yet.
    return $sliced_rs->all;
}

sub set_cache {
    my ($self, $cache) = @_;

    require Carp;
    Carp::croak("set_cache expects an arrayref of entries/objects")
        unless defined $cache && ref $cache eq 'ARRAY';

    # 1. Store as raw entries
    # This feeds line 129 in your 'all' method
    $self->{_entries} = $cache;

    # 2. Mark as prefetched
    # This triggers the inflation logic in line 129
    $self->{_is_prefetched} = 1;

    # 3. Clear any existing inflated rows and reset position
    # This ensures that if the cache is updated, the inflation happens again
    $self->{_rows} = undef;
    $self->{_pos}  = 0;

    return $self;
}

############################################################################

sub update {
    my $self = shift;
    my ($cond, $updates);

    # Logic to handle both:
    #   ->update({ col => val })
    #   ->update({ id => 1 }, { col => val })
    if (@_ > 1) {
        ($cond, $updates) = @_;
    } else {
        $updates = shift;
        $cond    = $self->{_cond}; # Use the ResultSet's internal filter
    }

    my $db = $self->{_async_db};
    my $inflators = $db->{_custom_inflators}{ $self->{_source_name} } || {};

    # Ensure nested Hashes are turned back into Strings for the database
    foreach my $col (keys %$updates) {
        if ($inflators->{$col} && $inflators->{$col}{deflate}) {
            $updates->{$col} = $inflators->{$col}{deflate}->($updates->{$col});
        }
    }

    # Dispatch via the main Async module's update handler
    # This uses the resolved $cond we figured out above
    return DBIx::Class::Async::update(
        $db,
        {
            source_name => $self->{_source_name},
            cond        => $cond,
            updates     => $updates,
        }
    );
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

############################################################################

sub _build_payload {
    my ($self, $cond, $attrs, $is_count_op) = @_;

    # 1. Condition Merging (Improved Literal Awareness)
    my $base_cond = $self->{_cond};
    my $new_cond  = $cond;
    my $merged_cond;

    if (ref($base_cond) eq 'HASH' && ref($new_cond) eq 'HASH') {
        $merged_cond = { %$base_cond, %$new_cond };
    }
    elsif (ref($new_cond) && ref($new_cond) ne 'HASH') {
        $merged_cond = $new_cond; # Literal SQL takes priority
    }
    else {
        $merged_cond = $new_cond // $base_cond // {};
    }

    # 2. Attribute Merging (Harden against non-HASH attrs)
    my $merged_attrs = (ref($self->{_attrs}) eq 'HASH' && ref($attrs) eq 'HASH')
        ? { %{$self->{_attrs}}, %{$attrs // {}} }
        : ($attrs // $self->{_attrs} // {});

    # 3. Only apply the Subquery Alias if we are specifically doing a COUNT
    # and there is a limit/offset involved.
    if ( $is_count_op
        && ( $merged_attrs->{rows}
             || $merged_attrs->{offset}
             || $merged_attrs->{limit} ) ) {
        $merged_attrs->{alias}       //= 'subquery_for_count';
        $merged_attrs->{is_subquery} //= 1;
    }

    return {
        source_name => $self->{_source_name},
        cond        => $merged_cond,
        attrs       => $merged_attrs,
    };
}

sub _find_reverse_relationship {
    my ($self, $source, $rel_source, $forward_rel) = @_;

    unless (ref $rel_source && $rel_source->can('relationships')) {
        confess("Critical Error: _find_reverse_relationship expected a ResultSource object but got: " . ($rel_source // 'undef'));
    }

    my @rel_names    = $rel_source->relationships;
    my $forward_info = $source->relationship_info($forward_rel);
    my $forward_cond = $forward_info->{cond};

    # 1. Extract keys from forward condition (e.g., 'foreign.user_id' => 'self.id')
    my ($forward_foreign, $forward_self);
    if (ref $forward_cond eq 'HASH') {
        my ($f, $s) = %$forward_cond;
        # Handle cases where value is a hash (like { -ident => 'id' })
        $s = $s->{'-ident'} // $s if ref $s eq 'HASH';

        $forward_foreign = $f =~ s/^foreign\.//r;
        $forward_self    = $s =~ s/^self\.//r;
    }

    # 2. Look for a relationship that points back to our source with matching keys
    foreach my $rev_rel (@rel_names) {
        my $rev_info       = $rel_source->relationship_info($rev_rel);
        my $rev_source_obj = $rel_source->related_source($rev_rel);

        # Check if this relationship points back to our original source
        next unless $rev_source_obj->source_name eq $source->source_name;

        # Validate the foreign keys match (in reverse)
        my $rev_cond = $rev_info->{cond};
        if (ref $rev_cond eq 'HASH') {
            my ($rev_foreign, $rev_self) = %$rev_cond;
            $rev_self = $rev_self->{'-ident'} // $rev_self if ref $rev_self eq 'HASH';

            my $rf_clean = $rev_foreign =~ s/^foreign\.//r;
            my $rs_clean = $rev_self    =~ s/^self\.//r;

            # The logic check:
            # If Forward is: Order(user_id) -> User(id)
            # Reverse must be: User(id) -> Order(user_id)
            if ($rf_clean eq $forward_self && $rs_clean eq $forward_foreign) {
                return $rev_rel;
            }
        }
    }

    # 3. Fallback: If we couldn't find it by key matching, try by source name only
    foreach my $rev_rel (@rel_names) {
        if ($rel_source->related_source($rev_rel)->source_name eq $source->source_name) {
            return $rev_rel;
        }
    }

    return undef;
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

sub _inflate_row {
    my ($self, $hash) = @_;
    return undef unless $hash;

    # Apply Column Inflation
    my $db          = $self->{_async_db};
    my $source_name = $self->{_source_name};
    my $inflators   = $db->{_custom_inflators}{$source_name} || {};

    foreach my $col (keys %$inflators) {
        if (exists $hash->{$col} && $inflators->{$col}{inflate}) {
            # This turns your JSON string back into a HASH ref!
            $hash->{$col} = $inflators->{$col}{inflate}->($hash->{$col});
        }
    }

    # Create the base row object
    my $row = $self->new_result($hash);
    $row->in_storage(1);

    # Inject Relationship Data (already perfect)
    for my $rel ($self->result_source->relationships) {
        if (exists $hash->{$rel}) {
            $row->{_relationship_data}{$rel} = $hash->{$rel};
        }
    }

    return $row;
}

1; # End of DBIx::Class::Async::ResultSet
