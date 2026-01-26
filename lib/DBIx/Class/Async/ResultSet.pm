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
use DBIx::Class::Async::ResultSetColumn;

use Data::Dumper;

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
                (ref($_) && $_->isa('DBIx::Class::Async::Row'))
                ? $_
                : $self->new_result($_, { in_storage => 1 })
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

    return DBIx::Class::Async::all($self->{_async_db}, {
        source_name => $self->{_source_name},
        cond        => $self->{_cond},
        attrs       => $self->{_attrs},
    })->then(sub {
        my $rows_data = shift;

        if (!ref($rows_data) || ref($rows_data) ne 'ARRAY') {
            return Future->done([]);
        }

        my @objects = map {
            $self->new_result($_, { in_storage => 1 })
        } @$rows_data;

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

############################################################################


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

sub result_source {
    my $self = shift;
    return $self->_get_source;
}

sub _get_source {
    my $self = shift;

    $self->{_source} ||= $self->{_schema_instance}->source($self->{_source_name});

    return $self->{_source};
}

############################################################################

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

############################################################################


############################################################################

sub single {
    my ($self) = @_;

    my $single_rs = $self->new_result_set({
        attrs => { %{ $self->{_attrs} || {} }, rows => 1 }
    });

    return $single_rs->next;
}

############################################################################

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
            ? $self->new_result($data, { in_storage => 1 })
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
        $self->{_pos} = 0;
        my $row = $self->{_rows}[$self->{_pos}++];

        # CRITICAL FIX: Wrap the result in a Future
        return Future->done($row);
    });
}

############################################################################

sub stats {
    my ($self, $key) = @_;

    # Return the whole stats hash if no key is provided
    return $self->{_async_db}->{_stats} unless $key;

    # Otherwise return the specific metric (e.g., 'queries')
    # Note: We map the public 'queries' to the internal '_queries'
    my $internal_key = $key =~ /^_/ ? $key : "_$key";
    return $self->{_async_db}->{_stats}->{$internal_key};
}

sub reset_stats {
    my $self = shift;
    foreach my $key (keys %{ $self->{_async_db}->{_stats} }) {
        $self->{_async_db}->{_stats}->{$key} = 0;
    }
    return $self;
}

############################################################################

sub schema {
    my $self = shift;
    return $self->{_schema};
}

############################################################################

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

sub _build_payload {
    my ($self, $cond, $attrs) = @_;

    # 1. Base Merge with Total Literal Awareness
    my $merged_cond;

    # If either the CURRENT condition or the NEW condition is a literal,
    # we generally can't merge them as hashes.
    if (ref($self->{_cond}) eq 'REF' || ref($self->{_cond}) eq 'SCALAR') {
        $merged_cond = $self->{_cond}; # Prioritize existing literal
    }
    elsif (ref($cond) eq 'REF' || ref($cond) eq 'SCALAR') {
        $merged_cond = $cond;          # Prioritize new literal
    }
    else {
        # Both are safe to treat as hashes (or undef)
        $merged_cond = { %{ $self->{_cond} || {} }, %{ $cond || {} } };
    }

    my $merged_attrs = { %{ $self->{_attrs} || {} }, %{ $attrs || {} } };

    # 2. The "Slice" Special Case (remains the same)
    if ( $merged_attrs->{rows} || $merged_attrs->{offset} || $merged_attrs->{limit} ) {
        $merged_attrs->{alias}       //= 'subquery_for_count';
        $merged_attrs->{is_subquery} //= 1;
    }

    return {
        source_name => $self->{_source_name},
        cond        => $merged_cond,
        attrs       => $merged_attrs,
    };
}

1; # End of DBIx::Class::Async::ResultSet
