package DBIx::Class::Async::Row;

use strict;
use warnings;
use utf8;
use v5.14;

use Carp;
use Future;
use Scalar::Util qw(blessed);


# PRIVATE CONSTANTS
# Protects internal attributes from being treated as database columns
my $INTERNAL_KEYS = qr/^(?:_.*|async_db|source_name|schema|_inflation_map)$/;

sub _mark_clean {
    my $self = shift;
    $self->{_dirty} = {};
    return $self;
}

sub new {
    my ($class, %args) = @_;

    croak "Missing required argument: schema_instance" unless $args{schema_instance};
    croak "Missing required argument: async_db"        unless $args{async_db};
    croak "Missing required argument: source_name"     unless $args{source_name};
    croak "Missing required argument: row_data"        unless $args{row_data};

    my $in_storage = delete $args{in_storage} // 0;
    my $data       = $args{row_data} || $args{_data} || {};

    my $self = bless {
        _schema_instance => $args{schema_instance},
        _async_db        => $args{async_db},
        _source_name     => $args{source_name},
        _result_source   => $args{result_source} // undef,
        _data            => { %$data },
        _dirty           => {},
        _inflated        => {},
        _related         => {},
        _in_storage      => $in_storage,
        _inflation_map   => {},
    }, $class;

    $self->_ensure_accessors;

    # WARM-UP: Pre-calculate metadata and shadow plain columns for speed
    my $source = $self->_get_source;
    if ($source && ref($source) && eval { $source->can('has_column') }) {
        foreach my $col (keys %$data) {
            # Skip internal plumbing
            next if $self->_is_internal($col);

            if ($source->has_column($col)) {
                my $info = $source->column_info($col);
                my $inflator = $info->{inflate} // 0;
                $self->{_inflation_map}{$col} = $inflator;

                # If NO inflator, shadow to top-level for direct hash-key speed
                if (!$inflator) {
                    $self->{$col} = $data->{$col};
                }
            }
            else {
                # Relationship or custom key - safe to shadow
                $self->{$col} = $data->{$col};
                $self->{_inflation_map}{$col} = 0;
            }
        }
    }

    return $self;
}


sub copy {
    my ($self, $changes) = @_;
    $changes //= {};

    my $source = $self->_get_source;

    # 1. Get ONLY keys that are valid database columns
    my %data;
    foreach my $col ($source->columns) {
        # Check _data first (clean storage), then top-level (hack storage)
        if (exists $self->{_data}{$col}) {
            $data{$col} = $self->{_data}{$col};
        }
        elsif (exists $self->{$col}) {
            # Ensure we don't accidentally copy our own management objects
            # if a column happened to have the same name
            next if $col =~ /^(?:async_db|schema|source_name|_source)$/;
            $data{$col} = $self->{$col};
        }
    }

    # 2. Remove Primary Keys (unless specifically overridden in $changes)
    foreach my $pk ($source->primary_columns) {
        delete $data{$pk} unless exists $changes->{$pk};
    }

    # 3. Apply user changes
    foreach my $col (keys %$changes) {
        $data{$col} = $changes->{$col};
    }

    # 4. Use the Async ResultSet to create
    return $self->{async_db}->resultset($self->{source_name})->create(\%data);
}

sub create_related {
    my ($self, $rel_name, $col_data) = @_;

    my $rs = $self->related_resultset($rel_name);

    my $rs_cond = (ref $rs->{cond} eq 'HASH')  ? $rs->{cond}
                : (ref $rs->{_cond} eq 'HASH') ? $rs->{_cond}
                : {};

    my $merged_data = { %$rs_cond, %{$col_data || {}} };

    return $rs->create($merged_data);
}

sub delete {
    my ($self) = @_;

    unless ($self->in_storage) {
        return Future->done(0);
    }

    # 1. Handle Single or Composite Primary Keys
    my @pk_cols = $self->{_result_source}->primary_columns;
    my %cond;

    foreach my $col (@pk_cols) {
        my $val = $self->get_column($col);
        croak "Cannot delete row: Primary key column '$col' is undefined"
            unless defined $val;
        $cond{$col} = $val;
    }

    if (my $db = $self->{_async_db}) {
        my $source = $self->{_source_name};

        # Clear the surgical/nested cache
        delete $db->{_query_cache}->{$source};

        # Clear the flat cache used by count/all
        if ($db->{_cache}) {
            foreach my $key (keys %{$db->{_cache}}) {
                if ($key =~ /^\Q$source\E\|/) {
                    delete $db->{_cache}->{$key};
                }
            }
        }
    }

    # 2. Use the Bridge (Ensure you use the key names the Worker expects)
    return DBIx::Class::Async::delete(
        $self->{_async_db},
        {
            source_name => $self->{_source_name},
            cond        => \%cond
        }
    )->then(sub {
        my $rows_affected = shift;

        # 3. Mark as no longer in DB
        $self->{_in_storage} = 0;

        # Return 1 for success (standard DBIC behavior)
        return Future->done($rows_affected + 0);
    });
}

sub _spawn_rs {
    my ($self) = @_;

    die "Cannot spawn ResultSet: missing _schema_instance or _source_name"
        unless $self->{_schema_instance} && $self->{_source_name};

    return DBIx::Class::Async::ResultSet->new(
        schema_instance => $self->{_schema_instance}, # Match ResultSet's new()
        async_db        => $self->{_async_db},
        source_name     => $self->{_source_name},
        cond            => $self->ident_condition,
    );
}

sub discard_changes {
    my $self = shift;

    # 1. Primary Key Validation (Matching your old design)
    my $source = $self->{_schema_instance}->resultset($self->{_source_name})->result_source;
    my @pk = $source->primary_columns;

    croak("Cannot discard changes on row without primary key") unless @pk;
    croak("Composite primary keys not yet supported") if @pk > 1;

    my $pk_col = $pk[0];
    my $id = $self->{_data}{$pk_col}; # Using new internal _data structure

    croak("Cannot discard changes: primary key value is undefined")
        unless defined $id;

    # 2. Fetch fresh data using the new re-anchored design
    # We use find($id) which returns a Future containing a new Row object
    return $self->_spawn_rs->find($id)->then(sub {
        my ($fresh_row) = @_;

        # In the new design, find() returns undef or a Row object
        unless ($fresh_row) {
            return Future->fail("Row vanished from database", 'db_error');
        }

        # 3. Synchronize internal state
        # We extract the columns from the fresh object into this one
        my $raw_data = $fresh_row->{_data};

        $self->{_data}  = { %$raw_data };
        $self->{_dirty} = {};

        # 4. Refresh speed shadows (Warm-up optimization)
        $self->_ensure_accessors;

        return Future->done($self);
    });
}

sub get_column {
    my ($self, $col) = @_;

    # 1. Fast-track internal plumbing
    return $self->{$col} if $self->_is_internal($col);

    # 2. Return cached inflated value if it exists
    return $self->{_inflated}{$col} if exists $self->{_inflated}{$col};

    # 3. Discovery & Exception Handling
    if (!exists $self->{_inflation_map}{$col}) {
        my $source = $self->_get_source;

        # If it's not in data AND not in schema, trigger DBIC exception
        if ($source && !$source->has_column($col) && !exists $self->{_data}{$col}) {
            # Calling column_info on a non-existent column triggers the "No such column" croak
            return $source->column_info($col);
        }

        if ($source && $source->has_column($col)) {
            $self->{_inflation_map}{$col} = $source->column_info($col)->{inflate} // 0;
        } else {
            $self->{_inflation_map}{$col} = 0;
        }
    }

    my $raw      = $self->{_data}{$col};
    my $inflator = $self->{_inflation_map}{$col};

    # 4. Inflate if needed
    if ($inflator && defined $raw) {
        # Check if already inflated via shadow key
        if ("$raw" =~ /^HASH\(0x/ && ref $self->{$col}) {
            return $self->{$col};
        }

        my $inflated = eval { $inflator->("$raw", $self) };
        if (!$@ && defined $inflated) {
            $self->{_inflated}{$col} = $inflated;
            $self->{$col} = $inflated;
            return $inflated;
        }
    }

    return $raw;
}


sub get_columns {
    my $self = shift;
    my $data = $self->{_data} || {};

    # 1. Get the list of valid column names from the source
    my @valid_cols = $self->_get_source->columns;

    # 2. Only extract keys that are actual database columns
    my %cols;
    foreach my $col (@valid_cols) {
        if (exists $data->{$col}) {
            $cols{$col} = $data->{$col};
        }
    }

    return wantarray ? %cols : \%cols;
}


sub get_dirty_columns {
    my $self = shift;

    my %dirty_values;
    foreach my $column (keys %{$self->{_dirty}}) {
        $dirty_values{$column} = $self->{_data}{$column};
    }

    return wantarray ? %dirty_values : \%dirty_values;
}

sub get_inflated_columns {
    my $self = shift;

    my %inflated;
    foreach my $col (keys %{$self->{_data}}) {
        $inflated{$col} = $self->get_column($col);
    }

    return %inflated;
}

sub id {
    my $self = shift;

    croak("id() cannot be called as a class method")
        unless ref $self;

    my $source = $self->_get_source;
    unless ($source && ref($source) && eval { $source->can('primary_columns') }) {
        croak("Could not retrieve valid ResultSource for " .
              ($self->{_source_name} // 'Unknown') .
              ". Source is either missing or unblessed.");
    }

    my @pk_columns = $source->primary_columns;

    croak("No primary key defined for " . $self->{source_name})
        unless @pk_columns;

    my @pk_values;
    foreach my $col (@pk_columns) {
        my $val = $self->get_column($col);

        # Warn if primary key is undefined (usually means row not in storage)
        unless (defined $val) {
            carp("Primary key column '$col' is undefined for " .
                 $self->{source_name});
        }

        push @pk_values, $val;
    }

    # Return based on context
    if (wantarray) {
        # List context: return list
        return @pk_values;
    } else {
        # Scalar context
        if (@pk_values == 1) {
            # Single primary key: return the value
            return $pk_values[0];
        } else {
            # Composite primary key: return arrayref
            return \@pk_values;
        }
    }
}

sub ident_condition {
    my $self = shift;
    my $source = $self->{_result_source}
               || $self->{_schema_instance}->resultset($self->{_source_name})->result_source;

    my @pks = $source->primary_columns;
    croak "ident_condition failed: No primary keys defined for " . $self->{_source_name} unless @pks;

    # Return a HASHREF instead of a list to make it easier to use elsewhere
    return { map { $_ => $self->{_data}{$_} } @pks };
}

sub insert {
    my $self = shift;

    # If the row is already in the database, DBIC behavior is to throw an error
    # or no-op. Here we follow the safer path.
    if ($self->in_storage) {
        return Future->fail("Check failed: count of objects to be inserted is 0 (already in storage)");
    }

    # update_or_insert handles the actual DB communication and state flipping
    return $self->update_or_insert;
}


sub insert_or_update {
    my $self = shift;
    return $self->update_or_insert(@_);
}


sub in_storage {
    my ($self, $val) = @_;

    if (defined $val) {
        $self->{_in_storage} = $val ? 1 : 0;
        # If it's in storage, it's no longer 'dirty' (unsaved changes)
        $self->{_dirty} = {} if $self->{_in_storage};
    }

    return $self->{_in_storage} // 0;
}


sub is_column_changed {
    my ($self, $column) = @_;

    croak("column name required") unless defined $column;

    return exists $self->{_dirty}{$column} ? 1 : 0;
}

sub is_column_dirty {
    my ($self, $column) = @_;
    return exists $self->{_dirty}{$column} ? 1 : 0;
}


sub make_column_dirty {
    my ($self, $column) = @_;

    croak("column name required") unless defined $column;

    $self->{_dirty}{$column} = 1;

    return $self;
}


sub related_resultset {
    my ($self, $rel_name, $cond, $attrs) = @_;

    # 1. Get metadata from the schema class (no temp_schema needed)
    my $source_obj = $self->{_async_db}->{_schema_class}->source($self->{_source_name});
    my $rel_info   = $source_obj->relationship_info($rel_name);
    die "No such relationship '$rel_name' on " . $self->{_source_name} unless $rel_info;

    # 2. Build the Join Condition (the foreign keys)
    my $join_cond = {};
    if ($rel_info->{cond}) {
        while (my ($foreign_col, $self_col) = each %{$rel_info->{cond}}) {
            $foreign_col =~ s/^foreign\.//;
            $self_col =~ s/^self\.//;
            $join_cond->{$foreign_col} = $self->get_column($self_col);
        }
    }

    # 3. Finalize condition and source name
    my $final_cond = { %$join_cond, %{ $cond || {} } };
    my $foreign_source_name = $rel_info->{source};

    # 4. Path A: Prefetched Data Cache
    if (exists $self->{_relationship_data}{$rel_name}) {
        my $prefetched = $self->{_relationship_data}{$rel_name};

        my $rs = DBIx::Class::Async::ResultSet->new(
             schema_instance => $self->{_schema_instance},
             async_db        => $self->{_async_db},
             source_name     => $foreign_source_name,
             cond            => $final_cond,    # Crucial for create_related
             attrs           => $attrs || {},
        );

        $rs->{_is_prefetched} = 1;
        $rs->{_entries}       = ref $prefetched eq 'ARRAY' ? $prefetched : [$prefetched];
        return $rs;
    }

    # 5. Path B: Standard Async Database Path
    return DBIx::Class::Async::ResultSet->new(
         schema_instance => $self->{_schema_instance},
         async_db        => $self->{_async_db},
         source_name     => $foreign_source_name,
         cond            => $final_cond,
         attrs           => $attrs || {},
    );
}

sub result_source {
    my $self = shift;
    return $self->_get_source;
}

sub set_column {
    my ($self, $col, $value) = @_;

    if (!defined $col || $col eq '') {
         require Carp;
         Carp::croak("Column name required for set_column");
    }

    # If someone tries to set 'async_db' or '_source', we return early
    # to protect the object's plumbing and prevent "dirty" poisoning.
    return $value if $self->_is_internal($col);

    # 1. Capture types before any operations occur
    my $old = $self->{_data}{$col};
    my $old_ref = ref($old)   || 'SCALAR';
    my $new_ref = ref($value) || 'SCALAR';

    my $changed = 0;
    if (!defined $old && defined $value) {
        $changed = 1;
    } elsif (defined $old && !defined $value) {
        $changed = 1;
    } elsif (defined $old && defined $value) {
        if ($old_ref ne 'SCALAR' || $new_ref ne 'SCALAR') {
            $changed = 1;
        } else {
            # Safe to use string comparison
            if ($old ne $value) {
                $changed = 1;
            }
        }
    }

    if ($changed) {
        $self->{_data}{$col} = $value;
        $self->{_dirty}{$col} = 1;

        # Clear the inflated cache so the next 'get_column' re-inflates the new data
        delete $self->{_inflated}{$col};

        # We delete the top-level key so it doesn't "shadow" the new data
        delete $self->{$col};
    }

    return $value;
}


sub set_columns {
    my ($self, $values) = @_;

    croak("hashref of column-value pairs required")
        unless defined $values && ref $values eq 'HASH';

    while (my ($column, $value) = each %$values) {
        $self->set_column($column, $value);
    }

    return $self;
}


sub update {
    my ($self, $values) = @_;

    # 1. Validation
    unless ($self->in_storage) {
        return Future->fail("Cannot update row: not in storage. Did you mean to call insert or update_or_insert?");
    }

    # 2. If values are passed, update internal state via set_column first
    if ($values) {
        croak("Usage: update({ col => val })") unless ref $values eq 'HASH';
        foreach my $col (keys %$values) {
            $self->set_column($col, $values->{$col});
        }
    }

    # 3. Delegate to update_or_insert
    # Since in_storage is true, update_or_insert will correctly
    # run the UPDATE logic and clear dirty flags on success.
    return $self->update_or_insert;
}


sub update_or_insert {
    my ($self, $data) = @_;

    my $async_db    = $self->{_async_db};
    my $source_name = $self->{_source_name};
    my $source      = $self->result_source;
    my ($pk_col)    = $source->primary_columns;

    # 1. Apply changes to the object
    if ($data && ref $data eq 'HASH') {
        foreach my $col (keys %$data) {
            $self->set_column($col, $data->{$col});
        }
    }

    my $is_update = $self->in_storage;

    # 2. Prepare Payload
    my %raw_payload = $is_update ? $self->get_dirty_columns : %{ $self->{_data} // {} };
    my %to_save;

    foreach my $col (keys %raw_payload) {
        # If it's not in _inflated, fall back to the raw value in %raw_payload.
        my $val  = exists $self->{_inflated}{$col} ? $self->{_inflated}{$col} : $raw_payload{$col};
        my $info = $source->column_info($col);

        # If a deflate handler exists and we have a reference, turn it into a string
        if ($info && $info->{deflate} && defined $val && ref $val) {
            $val = $info->{deflate}->($val, $self);
        }

        $to_save{$col} = $val;
    }

    # 3. Success handler
    my $on_success = sub {
        my ($res) = @_;

        # We check if it's an object FIRST before asking what kind of object it is.
        if (blessed($res) && $res->isa('DBIx::Class::Exception')) {
             return Future->fail($res->msg, 'db_error');
        }

        # Also check for the HASH-style error envelope which we saw in your logs
        if (ref $res eq 'HASH' && ($res->{error} || $res->{__error})) {
             my $err = $res->{error} // $res->{__error};
            return Future->fail($err, 'db_error');
        }

        # Normalise data source
        my $final_data;
        if (ref $res && ref $res eq 'HASH') {
            $final_data = $res;
        }
        elsif (ref $res && eval { $res->can('get_columns') }) {
            # Handle case where $res is another Row object
            my %cols = $res->get_columns;
            $final_data = \%cols;
        }
        else {
            # Scalar result (ID) or fallback
            $final_data = { %to_save };
            if (!$is_update && defined $res && !ref $res && $pk_col) {
                $final_data->{$pk_col} = $res;
            }
        }

        $self->{_in_storage} = 1;
        $self->in_storage(1);

        foreach my $col (keys %$final_data) {
            my $new_val = $final_data->{$col};

            # 1. Update core data first
            $self->{_data}{$col} = $new_val;

            # 1. Clear caches
            delete $self->{_inflated}{$col};
            delete $self->{_dirty}{$col};

            # 3. Handle the Shadow Key with a "Column Only" safety check
            my $source = $self->can('_get_source') ? $self->_get_source : undef;

            # CRITICAL: Only call column_info IF the source confirms it is a real column.
            # This skips 'schema', '_source', 'async_db', and relationships.
            if ($source && $source->has_column($col)) {
                my $info = $source->column_info($col);
                if ($info && $info->{inflate}) {
                    # For inflated cols, delete shadow to force get_column() to run.
                    delete $self->{$col};
                }
                else {
                    # For standard columns, update the shadow.
                    $self->{$col} = $new_val;
                }
            }
            else {
                # If it's an internal attribute or relationship,
                # just update the shadow if it already exists,
                # but NEVER ask column_info about it.
                $self->{$col} = $new_val if exists $self->{$col};
            }
        }

        return $self;
    };

    # 4. Dispatch
    if ($is_update) {
        return Future->done($self) unless keys %to_save;

        my $id_val = $self->{_data}{$pk_col}
            // ( $self->can($pk_col) ? $self->$pk_col : undef )
            // $self->{$pk_col};

        if (ref $id_val && eval { $id_val->can('get_column') }) {
            $id_val = $id_val->get_column($pk_col);
        }

        if (!defined $id_val) {
            return Future->fail(
                "Cannot update row: Primary key ($pk_col) is missing",
                "logic_error");
        }

        return DBIx::Class::Async::update(
            $self->{_async_db},
            {
                source_name => $source_name,
                cond        => { $pk_col => $id_val },
                updates     => \%to_save
            })->then(sub {
                my $res = shift;
                # Handle worker exceptions
                if (blessed($res) && $res->isa('DBIx::Class::Exception')) {
                    return Future->fail("$res", 'db_error');
                }
                return $on_success->($res);
            });
    } else {
        return DBIx::Class::Async::create(
            $self->{_async_db},
            {
                source_name => $source_name,
                data        => \%to_save
            }
            )->then(sub {
                my $res = shift;

                # 1. Catch Exception objects from the Worker
                if (blessed($res) && $res->isa('DBIx::Class::Exception')) {
                    return Future->fail("$res", 'db_error');
                }

                # 2. Hand off to your excellent on_success handler
                # This will set in_storage(1) and clear dirty flags.
                return $on_success->($res);
            });
    }
}

sub AUTOLOAD {
    my $self = shift;

    our $AUTOLOAD;
    my ($method) = $AUTOLOAD =~ /([^:]+)$/;

    # 1. Immediate exit for core/Future/Perl methods
    return if $method =~ /^(?:DESTROY|AWAIT_\w+|can|isa|then|get|on_\w+|failure|else|CLONE)$/;

    # 2. Handle Columns (Getter/Setter)
    # Ensure _get_source retrieves the metadata from the parent's schema cache
    my $source = eval { $self->_get_source };

    if ($source && $source->has_column($method)) {
        no strict 'refs';
        no warnings 'redefine';

        my $accessor = sub {
            my ($inner_self, $new_val) = @_;
            my $col_info = $inner_self->result_source->column_info($method);

            # SETTER MODE
            if (@_ > 1) {
                delete $inner_self->{_inflated}{$method};
                my $to_store = $new_val;

                if ($col_info->{deflate} && defined $new_val) {
                    $to_store = $col_info->{deflate}->($new_val, $inner_self);
                }

                $inner_self->set_column($method, $to_store);

                if ($col_info->{inflate} && ref $new_val) {
                    $inner_self->{_inflated}{$method} = $new_val;
                }
                return $new_val;
            }

            # GETTER MODE
            return $inner_self->{_inflated}{$method} if exists $inner_self->{_inflated}{$method};

            my $raw = $inner_self->get_column($method);
            if ($col_info->{inflate} && defined $raw) {
                my $inflated = $col_info->{inflate}->($raw, $inner_self);

                # Handle potential double-inflation edge cases
                if (!ref($inflated) && defined($inflated) && $inflated =~ /^mailto:mailto:/) {
                    $inner_self->{_inflated}{$method} = $raw;
                    return $raw;
                }

                $inner_self->{_inflated}{$method} = $inflated;
                return $inflated;
            }
            return $raw;
        };

        # Install into the specific class (likely the Anon hybrid class)
        my $target_class = ref($self);
        *{"${target_class}::$method"} = $accessor;
        return $self->$method(@_);
    }

    # 3. Handle Relationships
    if ($source && $source->can('relationship_info')) {
        my $rel_info = $source->relationship_info($method);
        if ($rel_info) {
            no strict 'refs';
            no warnings 'redefine';
            my $class = ref($self);
            *{"${class}::$method"} = sub {
                my ($inner_self, @args) = @_;
                return $inner_self->_fetch_relationship_async($method, $rel_info, @args);
            };
            return $self->$method(@_);
        }
    }

    # 4. Fallback for non-column data already in the buffer
    if (exists $self->{_data}{$method} && !@_) {
        return $self->{_data}{$method};
    }

    # ----------------------------------------------------------------------
    # 4.5 THE HIJACK GUARD
    # ----------------------------------------------------------------------
    # If we reached here, Async::Row doesn't recognize this as a DB column.
    # We check if a custom class further up the @ISA chain has this method.

    # 'can' looks through the entire inheritance tree (MRO)
    my $next_method = $self->can($method);

    if ($next_method && $next_method != \&AUTOLOAD) {
        # We found the real method (e.g., My::Custom::User::hello_name)!
        # Use goto to jump into it, preserving the caller context.
        unshift @_, $self;
        goto &$next_method;
    }

    # 5. Exception handling
    croak(sprintf(
        "Method '%s' not found in package '%s'. " .
        "(Can't locate object method via AUTOLOAD. " .
        "Is it a missing column or relationship in your ResultSource?)",
        $method, ref($self)
    ));
}

=head1 DESTROY

    # Called automatically when object is destroyed

Destructor method.

=cut

sub DESTROY {
    # Nothing to do
}

=head1 INTERNAL METHODS

These methods are for internal use and are documented for completeness.

=head2 _build_relationship_accessor

    my $coderef = $row->_build_relationship_accessor($method, $rel_info);

Builds an accessor for a relationship that checks for prefetched data first,
then falls back to lazy loading if needed. For has_many relationships, the
ResultSet object is cached in the row.

=cut

sub _build_relationship_accessor {
    my ($self, $rel_name, $rel_info) = @_;

    my $rel_type = $rel_info->{attrs}{accessor} || 'single';
    my $cond = $rel_info->{cond};

    if ($rel_type eq 'single' || $rel_type eq 'filter') {
        # belongs_to or might_have relationship
        return sub {
            my $row = shift;

            # 1. CHECK FOR PREFETCHED DATA FIRST
            if (exists $row->{_prefetched} && exists $row->{_prefetched}{$rel_name}) {
                my $prefetched = $row->{_prefetched}{$rel_name};
                return Future->done($prefetched) if blessed($prefetched);
                return Future->done(undef);
            }

            # 2. LAZY LOAD: Extract foreign key from condition
            my $fk = $row->_extract_foreign_key($cond);
            return Future->done(undef) unless $fk;

            my $fk_value = $row->get_column($fk->{self});
            return Future->done(undef) unless defined $fk_value;

            # 3. Fetch related row asynchronously via schema->resultset
            my $rel_source = $row->_get_source->related_source($rel_name);
            my $rel_rs = $row->{schema}->resultset($rel_source->source_name);

            return $rel_rs->find({ $fk->{foreign} => $fk_value });
        };

    } elsif ($rel_type eq 'multi') {
        # has_many relationship
        return sub {
            my $row = shift;
            my $extra_cond = shift || {};

            # Cache key for this relationship (includes extra conditions)
            my $cache_key = $rel_name;
            if (%$extra_cond) {
                # If there are extra conditions, create a unique cache key
                require Data::Dumper;
                local $Data::Dumper::Sortkeys = 1;
                local $Data::Dumper::Terse = 1;
                $cache_key .= '_' . Data::Dumper::Dumper($extra_cond);
            }

            # 1. CHECK FOR CACHED RESULTSET (without extra conditions)
            # Return cached ResultSet if it exists and no extra conditions were provided
            if (!%$extra_cond && exists $row->{_relationship_cache} && exists $row->{_relationship_cache}{$rel_name}) {
                return $row->{_relationship_cache}{$rel_name};
            }

            # 2. CHECK FOR PREFETCHED DATA
            if (exists $row->{_prefetched} && exists $row->{_prefetched}{$rel_name}) {
                my $prefetched_rs = $row->{_prefetched}{$rel_name};

                # If extra conditions are provided, filter the prefetched data
                if (%$extra_cond) {
                    # Don't cache filtered ResultSets
                    return $prefetched_rs->search($extra_cond);
                }

                # Cache the prefetched ResultSet
                $row->{_relationship_cache} ||= {};
                $row->{_relationship_cache}{$rel_name} = $prefetched_rs;

                return $prefetched_rs;
            }

            # 3. LAZY LOAD: Build the relationship condition
            my $fk = $row->_extract_foreign_key($cond);
            unless ($fk) {
                my $rel_source = $row->_get_source->related_source($rel_name);
                my $rs = $row->{schema}->resultset($rel_source->source_name)->search({});

                # Don't cache if we couldn't extract FK
                return $rs;
            }

            my $fk_value = $row->get_column($fk->{self});
            my $related_cond = { $fk->{foreign} => $fk_value, %$extra_cond };

            # 4. Create new ResultSet for lazy loading
            my $rel_source = $row->_get_source->related_source($rel_name);
            my $rs = $row->{async_db}->resultset($rel_source->source_name);

            # 5. Cache the ResultSet (only if no extra conditions)
            if (!%$extra_cond) {
                $row->{_relationship_cache} ||= {};
                $row->{_relationship_cache}{$rel_name} = $rs;
            }

            return $rs;
        };
    }

    # Default fallback
    return sub {
        require Carp;
        Carp::croak("Unknown relationship type for '$rel_name'");
    };
}

=head2 _ensure_accessors

    $row->_ensure_accessors;

Creates accessor methods for all columns in the result source.

=cut

sub _ensure_accessors {
    my $self   = shift;
    my $source = $self->_get_source;

    return unless blessed($source) && $source->can('can');

    my $class  = ref($self);

    return unless $class;
    #return if $class eq 'DBIx::Class::Async::Row';

    # 1. Handle Columns
    if ($source->can('columns')) {
        foreach my $col ($source->columns) {
            no strict 'refs';
            next if defined &{"${class}::$col"}; # Skip if already installed

            my $column_name = $col;
            no warnings 'redefine';
            *{"${class}::$column_name"} = sub {
                my $inner = shift;
                return @_ ? $inner->set_column($column_name, shift)
                          : $inner->get_column($column_name);
            };
        }
    }

    # 2. Handle Relationships
    if ($source->can('relationships')) {
        foreach my $rel ($source->relationships) {
            no strict 'refs';
            no warnings 'redefine';

            # Force redefine even if it exists (to clobber DBIC's sync method)
            my $rel_name = $rel;
            my $rel_info = $source->relationship_info($rel_name);

            *{"${class}::$rel_name"} = sub {
                my ($inner, @args) = @_;
                return $inner->_fetch_relationship_async($rel_name, $rel_info, @args);
            };
        }
    }
}

sub _fetch_relationship_async {
    my ($self, $rel_name, $rel_info, $attrs) = @_;

    # 1. Handle Metadata first
    $rel_info //= $self->_get_source->relationship_info($rel_name);
    my $acc_type  = $rel_info->{attrs}{accessor} // '';
    my $is_single = ($acc_type eq 'single' || $acc_type eq 'filter');

    # 2. Cache Hit Logic
    if (exists $self->{_related}{$rel_name}) {
        my $cached = $self->{_related}{$rel_name};
        # ONLY wrap in Future if it's a single row
        return $is_single ? Future->done($cached) : $cached;
    }

    # 3. Resolve search params
    my $target_source = $rel_info->{source};
    my $cond          = $rel_info->{cond};
    my %search_params;
    while (my ($f_key, $s_key) = each %$cond) {
        my $f_col = $f_key; $f_col =~ s/^foreign\.//;
        my $s_col = $s_key; $s_col =~ s/^self\.//;
        $search_params{$f_col} = $self->get_column($s_col);
    }

    if ($is_single) {
        return $self->{_schema_instance}->resultset($target_source)
            ->search(\%search_params, { %{$attrs // {}}, rows => 1 })
            ->next
            ->then(sub {
                my $row = shift;
                $self->{_related}{$rel_name} = $row;
                return Future->done($row);
            });
    }
    else {
        # Multi returns the RS directly (synchronously)
        my $rs = $self->{_schema_instance}->resultset($target_source)
                                          ->search(\%search_params, $attrs);

        $self->{_related}{$rel_name} = $rs;
        return $rs;
    }
}

=head2 _extract_foreign_key

Extracts foreign key mapping from a relationship condition.

=cut

sub _extract_foreign_key {
    my ($self, $cond) = @_;

    return undef unless $cond;

    # Handle simple foreign key condition: { 'foreign.id' => 'self.user_id' }
    if (ref $cond eq 'HASH') {
        my ($foreign_col) = keys %$cond;
        my $self_col = $cond->{$foreign_col};

        # Handle case where self_col is a reference (e.g., { '=' => 'self.user_id' })
        if (ref $self_col eq 'HASH') {
            # Extract the actual column name from the hash
            my ($op, $col) = %$self_col;
            $self_col = $col;
        }

        # Strip prefixes if present
        $foreign_col =~ s/^foreign\.//;
        $self_col =~ s/^self\.// if defined $self_col && !ref $self_col;

        return {
            foreign => $foreign_col,
            self => $self_col,
        };
    }

    # Handle code ref conditions (more complex relationships)
    # For now, we'll just return undef and let the relationship fail gracefully
    return undef;
}

=head2 _get_primary_key_info

    my $pk_info = $row->_get_primary_key_info;

Returns information about the primary key(s) for this row.

=over 4

=item B<Returns>

Hash reference with keys:
- C<columns>: Array reference of primary key column names
- C<count>: Number of primary key columns
- C<is_composite>: Boolean indicating composite primary key

=back

=cut

sub _get_primary_key_info {
    my $self   = shift;
    my $source = $self->_get_source or return;

    # CRITICAL: Call primary_columns in LIST context
    my @primary_columns = $source->primary_columns;

    return {
        columns      => \@primary_columns,
        count        => scalar @primary_columns,
        is_composite => scalar @primary_columns > 1,
    };
}

=head2 _get_source

    my $source = $row->_get_source;

Returns the result source for this row, loading it lazily if needed.

=cut

sub _get_source {
    my $self = shift;

    # 1. Return cached source if already found
    return $self->{_result_source} if $self->{_result_source};

    my $schema = $self->{_schema_instance};
    my $name   = $self->{_source_name};

    # 2. Guard: if we don't have a schema or name, we can't proceed
    return undef unless $schema && $name;

    # 3. Attempt to fetch from the schema
    my $source = eval { $schema->source($name) };

    # 4. Final verification: Only cache if it's a real, blessed object
    if ($source && blessed($source)) {
        return $self->{_result_source} = $source;
    }

    return undef;
}

#
#
# REALLY PRIVATE METHODS

sub _check_response {
    my ($self, $res) = @_;
    return undef unless ref $res;

    # 1. Handle DBIx::Class::Exception objects
    if (blessed($res) && $res->isa('DBIx::Class::Exception')) {
        return $res->msg;
    }

    # 2. Handle HashRef error envelopes ({ __error => "..." })
    if (ref $res eq 'HASH' && (my $err = $res->{error} // $res->{__error})) {
        return $err;
    }

    return undef;
}

sub _is_internal {
    my ($self, $col) = @_;

    return $col =~ $INTERNAL_KEYS;
}

1; # End of DBIx::Class::Async::Row
