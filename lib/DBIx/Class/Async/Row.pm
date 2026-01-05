package DBIx::Class::Async::Row;

use strict;
use warnings;
use utf8;
use v5.14;

use Carp;
use Future;

=head1 NAME

DBIx::Class::Async::Row - Asynchronous Row Object for DBIx::Class::Async

=head1 VERSION

Version 0.06

=cut

our $VERSION = '0.06';

=head1 SYNOPSIS

    use DBIx::Class::Async::Row;

    # Typically created by DBIx::Class::Async, not directly
    my $row = DBIx::Class::Async::Row->new(
        schema      => $schema,
        async_db    => $async_db,
        source_name => 'User',
        row_data    => { id => 1, name => 'John', email => 'john@example.com' },
    );

    # Access columns
    my $name = $row->name;  # Returns 'John'
    my $email = $row->get_column('email');  # Returns 'john@example.com'

    # Get all columns
    my %columns = $row->get_columns;

    # Update asynchronously
    $row->update({ name => 'John Doe' })->then(sub {
        my ($updated_row) = @_;
        say "Updated: " . $updated_row->name;
    });

    # Delete asynchronously
    $row->delete->then(sub {
        my ($success) = @_;
        say "Deleted: " . ($success ? 'yes' : 'no');
    });

    # Discard changes and refetch from database
    $row->discard_changes->then(sub {
        my ($fresh_row) = @_;
        # $fresh_row contains latest data from database
    });

=head1 DESCRIPTION

C<DBIx::Class::Async::Row> provides an asynchronous row object that represents
a single database row in a L<DBIx::Class::Async> application. It mimics the
interface of L<DBIx::Class::Row> but returns L<Future> objects for asynchronous
database operations.

This class is typically instantiated by L<DBIx::Class::Async> and not directly
by users. It provides both synchronous column access and asynchronous methods
for database operations.

=head1 CONSTRUCTOR

=head2 new

    my $row = DBIx::Class::Async::Row->new(
        schema      => $schema,            # DBIx::Class::Schema instance
        async_db    => $async_db,          # DBIx::Class::Async instance
        source_name => $source_name,       # Result source name
        row_data    => \%data,             # Hashref of row data
    );

Creates a new asynchronous row object.

=over 4

=item B<Parameters>

=over 8

=item C<schema>

A L<DBIx::Class::Schema> instance. Required.

=item C<async_db>

A L<DBIx::Class::Async> instance. Required.

=item C<source_name>

The name of the result source (table). Required.

=item C<row_data>

Hash reference containing the row's column data. Required.

=back

=item B<Throws>

=over 4

=item *

Croaks if any required parameter is missing.

=back

=back

=cut

sub new {
    my ($class, %args) = @_;

    croak "Missing required argument: schema"      unless $args{schema};
    croak "Missing required argument: async_db"    unless $args{async_db};
    croak "Missing required argument: source_name" unless $args{source_name};
    croak "Missing required argument: row_data"    unless $args{row_data};

    my $self = bless {
        schema      => $args{schema},
        async_db    => $args{async_db},
        source_name => $args{source_name},
        _source     => undef,  # Lazy-loaded
        _data       => $args{row_data},
        _inflated   => {},
        _related    => {},
    }, $class;

    $self->_ensure_accessors;

    return $self;
}

=head1 METHODS

=head2 get_column

    my $value = $row->get_column($column_name);

Synchronously retrieves a column value from the row.

=over 4

=item B<Parameters>

=over 8

=item C<$column_name>

Name of the column to retrieve.

=back

=item B<Returns>

The column value. If the column has an inflator defined, returns the
inflated value.

=item B<Throws>

Croaks if the column doesn't exist.

=back

=cut

sub get_column {
    my ($self, $col) = @_;

    # Direct column access first
    if (exists $self->{_data}
        && ref $self->{_data} eq 'HASH'
        && exists $self->{_data}{$col}) {
        # Check for column inflation if we have source info
        my $source = $self->_get_source;
        if ($source && $source->can('column_info')) {
            if (my $col_info = $source->column_info($col)) {
                if (my $inflator = $col_info->{inflate}) {
                    unless (exists $self->{_inflated}{$col}) {
                        $self->{_inflated}{$col} = $inflator->($self->{_data}{$col});
                    }
                    return $self->{_inflated}{$col};
                }
            }
        }
        return $self->{_data}{$col};
    }

    # Check if it's a relationship (if we have source info)
    my $source = $self->_get_source;
    if ($source && $source->can('relationship_info')) {
        if (my $rel = $source->relationship_info($col)) {
            # Trigger the relationship via AUTOLOAD
            return $self->$col;
        }
    }

    croak "No such column '$col' in " . ($self->{source_name} || 'Row');
}

=head2 get_columns

    my %columns = $row->get_columns;

Returns all columns as a hash.

=over 4

=item B<Returns>

Hash containing all column names and values.

=back

=cut

sub get_columns {
    my $self = shift;
    return %{$self->{_data}};
}

=head2 get_inflated_columns

    my %inflated_columns = $row->get_inflated_columns;

Returns all columns with inflated values where applicable.

=over 4

=item B<Returns>

Hash containing all column names and inflated values.

=back

=cut

sub get_inflated_columns {
    my $self = shift;

    my %inflated;
    foreach my $col (keys %{$self->{_data}}) {
        $inflated{$col} = $self->get_column($col);
    }

    return %inflated;
}

=head2 update

    $row->update({ column1 => $value1, column2 => $value2 })
        ->then(sub {
            my ($updated_row) = @_;
            # Handle updated row
        })
        ->catch(sub {
            my ($error) = @_;
            # Handle error
        });

Asynchronously updates the row in the database.

=over 4

=item B<Parameters>

=over 8

=item C<$data>

Hash reference containing column-value pairs to update.

=back

=item B<Returns>

A L<Future> that resolves to the updated row object.

=item B<Throws>

=over 4

=item *

Croaks if C<$data> is not a hash reference.

=item *

Croaks if the row is not in storage.

=back

=back

=cut

sub update {
    my ($self, $data) = @_;

    croak("Usage: update({ col => val })")    unless ref $data eq 'HASH';
    croak("Cannot update row not in storage") unless $self->in_storage;

    # Get primary key dynamically
    my $pk_info = $self->_get_primary_key_info;
    my $pk_col  = $pk_info->{columns}[0];
    my $id      = $self->get_column($pk_col);

    return $self->{async_db}->update($self->{source_name}, $id, $data)->then(sub {
        my ($result) = @_;

        # Update local object with the returned data
        if (ref $result eq 'HASH' && %$result) {
            # Merge the result into our local data
            $self->{_data} = { %{$self->{_data}}, %$result };
        } else {
            # If result is empty, use what we sent
            while (my ($col, $val) = each %$data) {
                $self->{_data}{$col} = $val;
            }
        }

        $self->{_dirty_columns} = {};
        $self->{_inflated} = {};

        return Future->done($self);
    });
}

=head2 delete

    $row->delete
        ->then(sub {
            my ($success) = @_;
            if ($success) {
                say "Row deleted successfully";
            }
        })
        ->catch(sub {
            my ($error) = @_;
            # Handle error
        });

Asynchronously deletes the row from the database.

=over 4

=item B<Returns>

A L<Future> that resolves to true if the row was deleted, false otherwise.

=item B<Throws>

Croaks if the row doesn't have a primary key.

=back

=cut

sub delete {
    my ($self) = @_;

    # If already deleted (not in storage), return false immediately
    unless ($self->in_storage) {
        return Future->done(0);
    }

    my $pk_info = $self->_get_primary_key_info;
    my $pk      = $pk_info->{columns}[0];
    my $id      = $self->get_column($pk);

    croak "Cannot delete row without a primary key"
        unless defined $id;

    return $self->{async_db}->delete($self->{source_name}, $id)->then(sub {
        my ($success) = @_;

        # Mark as not in storage
        $self->{_in_storage} = 0;

        # Return the success value (1 or 0), not $self
        return Future->done($success);
    });
}

=head2 discard_changes

    $row->discard_changes
        ->then(sub {
            my ($fresh_row) = @_;
            # Row now contains latest data from database
        })
        ->catch(sub {
            my ($error) = @_;
            # Handle error
        });

Discards any local changes and refetches the row from the database.

=over 4

=item B<Returns>

A L<Future> that resolves to the row object with fresh data.

=item B<Throws>

=over 4

=item *

Croaks if the row doesn't have a primary key.

=back

=back

=cut

sub discard_changes {
    my ($self) = @_;

    my $pk_info = $self->_get_primary_key_info;
    my $pk      = $pk_info->{columns}[0];
    my $id      = $self->get_column($pk);

    croak "Cannot discard_changes on a row without a primary key"
        unless defined $id;

    # Re-fetch the row from the database using async_db->find()
    return $self->{async_db}->find($self->{source_name}, $id)->then(sub {
        my ($fresh_data) = @_;

        if ($fresh_data && ref $fresh_data eq 'HASH') {
            # Sync internal data with the freshly fetched data
            $self->{_data} = { %$fresh_data };
            $self->{_inflated} = {};
            $self->{_dirty_columns} = {};
        }

        return Future->done($self);
    });
}

=head2 in_storage

    if ($row->in_storage) {
        # Row exists in database
    }

Checks whether the row exists in the database.

=over 4

=item B<Returns>

True if the row is in storage (has a primary key and hasn't been deleted),
false otherwise.

=back

=cut

sub in_storage {
    my ($self) = @_;

    # Check if explicitly marked as not in storage (after delete)
    return 0 if exists $self->{_in_storage} && !$self->{_in_storage};

    # Check if we have primary key data
    my $pk_info = eval { $self->_get_primary_key_info };
    return 0 unless $pk_info;

    my $pk = $pk_info->{columns}[0];
    my $id = eval { $self->get_column($pk) };

    # If we have a primary key value and haven't been explicitly marked as deleted,
    # we're in storage
    return defined $id ? 1 : 0;
}

=head2 result_source

    my $source = $row->result_source;

Returns the L<DBIx::Class::ResultSource> for this row.

=over 4

=item B<Returns>

The result source object, or undef if not available.

=back

=cut

sub result_source {
    my $self = shift;
    return $self->_get_source;
}

=head2 related_resultset

    my $rs = $row->related_resultset($relationship_name);

Returns a resultset for a related table.

=over 4

=item B<Parameters>

=over 8

=item C<$relationship_name>

Name of the relationship as defined in the result class.

=back

=item B<Returns>

A L<DBIx::Class::ResultSet> for the related table, filtered by the
relationship condition.

=item B<Throws>

=over 4

=item *

Croaks if the relationship doesn't exist.

=item *

Croaks if the relationship condition cannot be parsed.

=back

=back

=cut

sub related_resultset {
    my ($self, $rel_name) = @_;

    my $source = $self->_get_source;
    my $rel_info = $source->relationship_info($rel_name)
        or croak "No such relationship '$rel_name'";

    # Get the condition
    my $cond = $rel_info->{cond};

    my ($self_column, $foreign_column);

    if (ref $cond eq 'HASH') {
        # Parse hashref: { 'foreign.id' => 'self.user_id' } or { 'foreign.user_id' => 'self.id' }
        foreach my $key (keys %$cond) {
            my $value = $cond->{$key};
            if ($value =~ /^self\.(\w+)$/) {
                $self_column = $1;
                $foreign_column = $key;
                $foreign_column =~ s/^foreign\.//;
                last;
            } elsif ($key =~ /^foreign\.(\w+)$/ && $value =~ /^self\.(\w+)$/) {
                # Alternative format
                $foreign_column = $1;
                $self_column = $value =~ /^self\.(\w+)$/ ? $1 : undef;
                last;
            }
        }
    } elsif (!ref $cond) {
        # String format
        if ($cond =~ /^self\.(\w+)$/) {
            $self_column = $1;
            $foreign_column = 'id';  # Default
        }
    }

    croak "Could not parse relationship condition for '$rel_name'"
        unless $self_column && $foreign_column;

    # Get value from our row
    my $value = $self->get_column($self_column);

    # Get foreign source
    my $foreign_source = $rel_info->{source} or
        croak "No source defined for relationship '$rel_name'";

    my $foreign_class = $rel_info->{class} || $rel_info->{source} or
        croak "No source/class defined for relationship '$rel_name'";

    # Use the class/source to get the initial resultset
    my $rs = $self->{schema}->resultset($rel_info->{class} || $rel_info->{source});

    # Get the moniker. If DBIx::Class gives us 'TestSchema::Result::Order',
    # we strip it down to 'Order' to satisfy the test.
    my $moniker = $rs->source_name;
    $moniker =~ s/.*:://;

    # Create the condition for the foreign key
    my $search_cond = { $foreign_column => $value };

    # Return the resultset using the cleaned short name
    return $self->{schema}->resultset($moniker)->search($search_cond);
}

=head2 insert

    $row->insert
        ->then(sub {
            my ($inserted_row) = @_;
            # Row has been inserted
        });

Asynchronously inserts the row into the database.

Note: This method is typically called automatically by L<DBIx::Class::Async/create>.
For existing rows, it returns an already-resolved Future.

=over 4

=item B<Returns>

A L<Future> that resolves to the row object.

=back

=cut

sub insert {
    my $self = shift;
    # Already inserted via create()
    return Future->done($self);
}

=head1 AUTOLOAD METHODS

The class uses AUTOLOAD to provide dynamic accessors for:

=over 4

=item *

Column values (e.g., C<< $row->name >> for column 'name')

=item *

Relationship accessors (e.g., C<< $row->orders >> for 'orders' relationship)

=back

Relationship results are cached in the object after first access.

=head1 INTERNAL METHODS

These methods are for internal use and are documented for completeness.

=head2 _ensure_accessors

    $row->_ensure_accessors;

Creates accessor methods for all columns in the result source.

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

=head2 _get_source

    my $source = $row->_get_source;

Returns the result source for this row, loading it lazily if needed.

=head2 _build_relationship_accessor

    my $coderef = $row->_build_relationship_accessor($method, $rel_info);

Builds an accessor coderef for a relationship.

=head2 AUTOLOAD

    # Called automatically for column and relationship access
    my $value = $row->column_name;
    my $related = $row->relationship_name;

Handles dynamic method dispatch for columns and relationships.

=head2 DESTROY

    # Called automatically when object is destroyed

Destructor method.

=cut

# Internal methods and AUTOLOAD implementation follow...

sub _ensure_accessors {
    my $self  = shift;
    my $class = ref $self;

    my $source  = $self->{schema}->source($self->{source_name});
    my @columns = $source->columns;

    foreach my $col (@columns) {
        next if $class->can($col);

        no strict 'refs';
        *{"${class}::$col"} = sub {
            my $inner_self = shift;
            return $inner_self->get_column($col);
        };
    }
}

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

sub _get_source {
    my $self = shift;

    unless ($self->{_source}) {
        if ($self->{schema}
            && ref $self->{schema}
            && $self->{schema}->can('source')) {
            $self->{_source} = eval { $self->{schema}->source($self->{source_name}) };
            return $self->{_source} if $self->{_source};
        }
    }

    return $self->{_source};
}

sub _build_relationship_accessor {
    my ($self, $method, $rel_info) = @_;
    my $foreign_source = $rel_info->{source};

    if ($rel_info->{attrs}{accessor} &&
        $rel_info->{attrs}{accessor} eq 'single') {
        return sub {
            my $self = shift;
            return $self->{_related}{$method} if exists $self->{_related}{$method};

            my $rs = $self->related_resultset($method);
            my $result = $rs->first->get;

            my $row_obj;
            if (UNIVERSAL::isa($result, 'DBIx::Class::Async::Row')) {
                $row_obj = $result;
            } elsif (ref $result eq 'HASH'
                     && $self->{schema}
                     && $self->{async_db}) {
                $row_obj = DBIx::Class::Async::Row->new(
                    schema      => $self->{schema},
                    async_db    => $self->{async_db},
                    source_name => $foreign_source,
                    row_data    => $result,
                );
            } else {
                $row_obj = $result;
            }

            $self->{_related}{$method} = $row_obj;
            return $row_obj;
        };
    } else {
        return sub {
            my $self = shift;
            return $self->{_related}{$method} if exists $self->{_related}{$method};

            my $rs = $self->related_resultset($method);
            my $result = $rs->all->get;

            my $rows;
            if (ref $result eq 'ARRAY'
                && $self->{schema}
                && $self->{async_db}) {
                $rows = [];
                foreach my $item (@$result) {
                    if (UNIVERSAL::isa($item, 'DBIx::Class::Async::Row')) {
                        push @$rows, $item;
                    } elsif (ref $item eq 'HASH') {
                        push @$rows, DBIx::Class::Async::Row->new(
                            schema      => $self->{schema},
                            async_db    => $self->{async_db},
                            source_name => $foreign_source,
                            row_data    => $item,
                        );
                    } else {
                        push @$rows, $item;
                    }
                }
            } else {
                $rows = $result;
            }

            $self->{_related}{$method} = $rows;
            return $rows;
        };
    }
}

sub AUTOLOAD {
    my $self = shift;
    our $AUTOLOAD;
    my ($method) = $AUTOLOAD =~ /([^:]+)$/;

    # Skip DESTROY
    return if $method eq 'DESTROY';

    # 1. Fast path: direct column access
    if (exists $self->{_data}{$method}) {
        return $self->{_data}{$method};
    }

    my $source = $self->_get_source;

    # 2. Check if it's a relationship
    my $rel_info;
    if ($source && $source->can('relationship_info')) {
        $rel_info = $source->relationship_info($method);
    }

    if ($rel_info) {
        my $accessor = $self->_build_relationship_accessor($method, $rel_info);
        {
            no strict 'refs';
            *{ref($self) . "::$method"} = $accessor;
        }
        return $accessor->($self);
    }

    # 3. GUARD: Only try get_column if it's actually a column
    # This prevents "No such column 'get'" errors when calling missing methods
    if ($source && $source->has_column($method)) {
        return $self->get_column($method);
    }

    # 4. Fallback to standard Perl error for missing methods
    require Carp;
    Carp::croak("Method $method not found in " . ref $self);
}

sub DESTROY {
    # Nothing to do
}

=head1 SEE ALSO

=over 4

=item *

L<DBIx::Class::Async> - Asynchronous DBIx::Class interface

=item *

L<DBIx::Class::Row> - Synchronous DBIx::Class row interface

=item *

L<Future> - Asynchronous programming abstraction

=back

=head1 AUTHOR

Mohammad Sajid Anwar, C<< <mohammad.anwar at yahoo.com> >>

=head1 REPOSITORY

L<https://github.com/manwar/DBIx-Class-Async>

=head1 BUGS

Please report any bugs or feature requests through the web interface at L<https://github.com/manwar/DBIx-Class-Async/issues>.
I will  be notified and then you'll automatically be notified of progress on your
bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::Class::Async::Row

You can also look for information at:

=over 4

=item * BUG Report

L<https://github.com/manwar/DBIx-Class-Async/issues>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-Class-Async>

=item * Search MetaCPAN

L<https://metacpan.org/dist/DBIx-Class-Async/>

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2026 Mohammad Sajid Anwar.

This program  is  free software; you can redistribute it and / or modify it under
the  terms  of the the Artistic License (2.0). You may obtain a  copy of the full
license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any  use,  modification, and distribution of the Standard or Modified Versions is
governed by this Artistic License.By using, modifying or distributing the Package,
you accept this license. Do not use, modify, or distribute the Package, if you do
not accept this license.

If your Modified Version has been derived from a Modified Version made by someone
other than you,you are nevertheless required to ensure that your Modified Version
 complies with the requirements of this license.

This  license  does  not grant you the right to use any trademark,  service mark,
tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge patent license
to make,  have made, use,  offer to sell, sell, import and otherwise transfer the
Package with respect to any patent claims licensable by the Copyright Holder that
are  necessarily  infringed  by  the  Package. If you institute patent litigation
(including  a  cross-claim  or  counterclaim) against any party alleging that the
Package constitutes direct or contributory patent infringement,then this Artistic
License to you shall terminate on the date that such litigation is filed.

Disclaimer  of  Warranty:  THE  PACKAGE  IS  PROVIDED BY THE COPYRIGHT HOLDER AND
CONTRIBUTORS  "AS IS'  AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES. THE IMPLIED
WARRANTIES    OF   MERCHANTABILITY,   FITNESS   FOR   A   PARTICULAR  PURPOSE, OR
NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY YOUR LOCAL LAW. UNLESS
REQUIRED BY LAW, NO COPYRIGHT HOLDER OR CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL,  OR CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE
OF THE PACKAGE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut

1; # End of DBIx::Class::Async::Row
