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

    # Inherit from parent if not provided in args
    my $new_obj = {
        _async_db     => $args->{async_db}    // $self->{_async_db},
        _source_name  => $args->{source_name} // $self->{_source_name},
        _schema       => $args->{schema}      // $self->{_schema},
        _result_class => $args->{result_class} // $self->{_result_class},
        _cond         => $args->{cond}        // {},
        _attrs        => $args->{attrs}       // {},
        _rows         => undef,
        _pos          => $args->{pos}         // 0,
        _pager        => $args->{pager},
        _entries      => $args->{entries},
        _is_prefetched => $args->{is_prefetched} // 0,
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

sub count {
    my ($self, $cond, $attrs) = @_;

    my $db = $self->{_async_db};
    $db->{_stats}{_queries}++;

    my $payload = $self->_build_payload($cond, $attrs);

    warn "[PID $$] STAGE 1 (Parent): Dispatching count";

    # This returns a Future that will be resolved by the worker
    return DBIx::Class::Async::count($db, $payload);
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

sub find {
    my ($self, $id) = @_;
    warn "[PID $$] find() called with id=$id";
    return Future->done(undef) unless defined $id;

    # search_rs creates a NEW resultset limited to this ID
    my $rs = $self->search_rs({ id => $id });
    warn "[PID $$] find() created search_rs, _rows=" . (defined $rs->{_rows} ? ref($rs->{_rows}) || 'SCALAR:' . $rs->{_rows} : 'undef');

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

        return Future->done($self->{_rows}[$self->{_pos}++]);
    });
}

1; # End of DBIx::Class::Async::ResultSet
