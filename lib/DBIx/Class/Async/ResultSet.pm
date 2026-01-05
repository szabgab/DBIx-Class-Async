package DBIx::Class::Async::ResultSet;

use strict;
use warnings;
use utf8;
use v5.14;

use Carp;
use Future;
use Scalar::Util 'blessed';
use DBIx::Class::Async::Row;

=head1 NAME

DBIx::Class::Async::ResultSet - Asynchronous ResultSet for DBIx::Class::Async

=head1 VERSION

Version 0.06

=cut

our $VERSION = '0.06';

=head1 SYNOPSIS

    use DBIx::Class::Async::ResultSet;

    # Typically obtained from DBIx::Class::Async::Schema
    my $rs = $schema->resultset('User');

    # Synchronous methods (return Future objects)
    $rs->all->then(sub {
        my ($users) = @_;
        foreach my $user (@$users) {
            say "User: " . $user->name;
        }
    });

    $rs->search({ active => 1 })->count->then(sub {
        my ($count) = @_;
        say "Active users: $count";
    });

    # Asynchronous future methods
    $rs->all_future->then(sub {
        my ($data) = @_;
        # Raw data arrayref
    });

    # Chaining methods
    $rs->search({ status => 'active' })
       ->order_by('created_at')
       ->rows(10)
       ->all->then(sub {
           my ($active_users) = @_;
           # Process results
       });

    # Create new records
    $rs->create({
        name  => 'Alice',
        email => 'alice@example.com',
    })->then(sub {
        my ($new_user) = @_;
        say "Created user ID: " . $new_user->id;
    });

=head1 DESCRIPTION

C<DBIx::Class::Async::ResultSet> provides an asynchronous result set interface
for L<DBIx::Class::Async>. It mimics the L<DBIx::Class::ResultSet> API but
returns L<Future> objects for database operations, allowing non-blocking
asynchronous database access.

This class supports both synchronous-style iteration (using C<next> and C<reset>)
and asynchronous operations (using C<then> callbacks). All database operations
are delegated to the underlying L<DBIx::Class::Async> instance.

=head1 CONSTRUCTOR

=head2 new

    my $rs = DBIx::Class::Async::ResultSet->new(
        schema      => $schema,            # DBIx::Class::Schema instance
        async_db    => $async_db,          # DBIx::Class::Async instance
        source_name => $source_name,       # Result source name
    );

Creates a new asynchronous result set.

=over 4

=item B<Parameters>

=over 8

=item C<schema>

A L<DBIx::Class::Schema> instance. Required.

=item C<async_db>

A L<DBIx::Class::Async> instance. Required.

=item C<source_name>

The name of the result source (table). Required.

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

    return bless {
        schema        => $args{schema},
        async_db      => $args{async_db},
        source_name   => $args{source_name},
        _source       => undef,  # Lazy-loaded
        _cond         => {},
        _attrs        => {},
        _rows         => undef,
        _pos          => 0,
    }, $class;
}

=head1 METHODS

=head2 all

    $rs->all->then(sub {
        my ($rows) = @_;
        # $rows is an arrayref of DBIx::Class::Async::Row objects
    });

Returns all rows matching the current search criteria as L<DBIx::Class::Async::Row>
objects.

=over 4

=item B<Returns>

A L<Future> that resolves to an array reference of L<DBIx::Class::Async::Row>
objects.

=item B<Notes>

Results are cached internally for use with C<next> and C<reset> methods.

=back

=cut

sub all {
    my $self = shift;

    my $source_name = $self->{source_name};

    return $self->{async_db}->search(
        $source_name,
        $self->{_cond},
        $self->{_attrs}
    )->then(sub {
        my ($rows_data) = @_;

        my @rows = map {
            DBIx::Class::Async::Row->new(
                schema       => $self->{schema},
                async_db     => $self->{async_db},
                source_name  => $source_name,
                row_data     => $_,
            )
        } @$rows_data;

        # Cache results for next() iteration
        $self->{_rows} = \@rows;
        $self->{_pos} = 0;

        return Future->done(\@rows);
    });
}

=head2 all_future

    $rs->all_future->then(sub {
        my ($data) = @_;
        # $data is an arrayref of raw hashrefs
    });

Returns all rows matching the current search criteria as raw data.

=over 4

=item B<Returns>

A L<Future> that resolves to an array reference of hash references containing
raw row data.

=item B<Notes>

This method bypasses row object creation for performance. Use C<all> if you
need L<DBIx::Class::Async::Row> objects.

=back

=cut

sub all_future {
    my $self = shift;

    return $self->{async_db}->search(
        $self->{source_name},
        $self->{_cond},
        $self->{_attrs}
    )->then(sub {
        my ($rows_data) = @_;
        # Store raw data so iterator methods can use them without re-fetching
        $self->{_rows}  = $rows_data;
        $self->{_pos}   = 0;
        return Future->done($rows_data);
    });
}

=head2 count

    $rs->count->then(sub {
        my ($count) = @_;
        say "Found $count rows";
    });

Returns the count of rows matching the current search criteria.

=over 4

=item B<Returns>

A L<Future> that resolves to the number of matching rows.

=back

=cut

sub count {
    my $self = shift;

    return $self->{async_db}->count(
        $self->{source_name},
        $self->{_cond},
    );
}

=head2 count_future

    $rs->count_future->then(sub {
        my ($count) = @_;
        # Same as count(), alias for API consistency
    });

Alias for C<count>. Returns the count of rows matching the current search criteria.

=over 4

=item B<Returns>

A L<Future> that resolves to the number of matching rows.

=back

=cut

sub count_future {
    my $self = shift;

    return $self->{async_db}->count(
        $self->{source_name},
        $self->{_cond}
    );
}

=head2 create

    $rs->create({ name => 'Alice', email => 'alice@example.com' })
       ->then(sub {
           my ($new_row) = @_;
           say "Created row ID: " . $new_row->id;
       });

Creates a new row in the database.

=over 4

=item B<Parameters>

=over 8

=item C<$data>

Hash reference containing column-value pairs for the new row.

=back

=item B<Returns>

A L<Future> that resolves to a L<DBIx::Class::Async::Row> object representing
the newly created row.

=back

=cut

sub create {
    my ($self, $data) = @_;

    return $self->{async_db}->create(
        $self->{source_name},
        $data,
    )->then(sub {
        my ($row_data) = @_;

        return Future->done(
            DBIx::Class::Async::Row->new(
                schema       => $self->{schema},
                async_db     => $self->{async_db},
                source_name  => $self->{source_name},
                row_data     => $row_data,
            )
        );
    });
}

=head2 delete

    $rs->search({ status => 'inactive' })->delete->then(sub {
        my ($deleted_count) = @_;
        say "Deleted $deleted_count rows";
    });

Deletes all rows matching the current search criteria.

=over 4

=item B<Returns>

A L<Future> that resolves to the number of rows deleted.

=item B<Notes>

This method fetches all matching rows first to count them and get their IDs,
then deletes them individually. For large result sets, consider using a direct
SQL delete via the underlying database handle.

=back

=cut

sub delete {
    my $self = shift;

    # Get all rows to count them and get their IDs
    return $self->all_future->then(sub {
        my ($rows) = @_;

        # If no rows, return 0
        return Future->done(0) unless @$rows;

        # Delete each row
        my @futures;
        my @pk = $self->result_source->primary_columns;

        foreach my $row_data (@$rows) {
            my $id = $row_data->{$pk[0]};
            push @futures, $self->{async_db}->delete($self->{source_name}, $id);
        }

        return Future->wait_all(@futures)->then(sub {
            # Count successful deletes
            my $deleted_count = 0;
            foreach my $f (@_) {
                my $result = eval { $f->get };
                $deleted_count++ if $result;
            }
            return Future->done($deleted_count);
        });
    });
}

=head2 find

    $rs->find($id)->then(sub {
        my ($row) = @_;
        if ($row) {
            say "Found: " . $row->name;
        } else {
            say "Not found";
        }
    });

Finds a single row by primary key.

=over 4

=item B<Parameters>

=over 8

=item C<$id>

Primary key value, or hash reference for composite primary key lookup.

=back

=item B<Returns>

A L<Future> that resolves to a L<DBIx::Class::Async::Row> object if found,
or C<undef> if not found.

=item B<Throws>

=over 4

=item *

Dies if composite primary key is not supported.

=back

=back

=cut

sub find {
    my ($self, @args) = @_;

    my $cond;

    # Scalar -> primary key lookup (DBIC semantics)
    if (@args == 1 && !ref $args[0]) {
        my @pk = $self->result_source->primary_columns;
        die "Composite PK not supported" if @pk != 1;

        $cond = { $pk[0] => $args[0] };
    }
    else {
        # Hashref or complex condition
        $cond = $args[0];
    }

    # Fully async: search builds query, single_future executes async
    return $self->search($cond)->single_future;
}

=head2 first

    $rs->first->then(sub {
        my ($row) = @_;
        if ($row) {
            say "First row: " . $row->name;
        }
    });

Returns the first row matching the current search criteria.

=over 4

=item B<Returns>

A L<Future> that resolves to a L<DBIx::Class::Async::Row> object if found,
or C<undef> if no rows match.

=back

=cut

sub first {
    my $self = shift;

    return $self->search(undef, { rows => 1 })->all_future->then(sub {
        my ($rows_arrayref) = @_;

        if (@$rows_arrayref > 0) {
            my $row_obj = DBIx::Class::Async::Row->new(
                schema      => $self->{schema},
                async_db    => $self->{async_db},
                source_name => $self->{source_name},
                row_data    => $rows_arrayref->[0],
            );
            return Future->done($row_obj);
        }

        return Future->done(undef);
    });
}

=head2 get

    my $rows = $rs->get;
    # Returns cached rows, or empty arrayref if not fetched

Returns the currently cached rows.

=over 4

=item B<Returns>

Array reference of cached rows (either raw data or row objects, depending on
how they were fetched).

=item B<Notes>

This method returns immediately without performing any database operations.
It only returns data that has already been fetched via C<all>, C<all_future>,
or similar methods.

=back

=cut

sub get {
    my $self = shift;
    # Returns current cached rows (raw or objects)
    return $self->{_rows} || [];
}

=head2 get_column

    $rs->get_column('name')->then(sub {
        my ($names) = @_;
        # $names is an arrayref of name values
    });

Returns values from a single column for all rows matching the current criteria.

=over 4

=item B<Parameters>

=over 8

=item C<$column>

Column name to retrieve values from.

=back

=item B<Returns>

A L<Future> that resolves to an array reference of column values.

=back

=cut

sub get_column {
    my ($self, $column) = @_;

    return $self->all->then(sub {
        my (@rows) = @_;

        my @values = map { $_->get_column($column) } @rows;
        return Future->done(\@values);
    });
}

=head2 next

    while (my $row = $rs->next) {
        say "Row: " . $row->name;
    }

Returns the next row from the cached result set.

=over 4

=item B<Returns>

A L<DBIx::Class::Async::Row> object, or C<undef> when no more rows are available.

=item B<Notes>

If no rows have been fetched yet, this method performs a blocking fetch via
C<all>. The results are cached for subsequent C<next> calls. Call C<reset>
to restart iteration.

=back

=cut

sub next {
    my $self = shift;

    # If we haven't fetched yet, do a blocking fetch
    unless ($self->{_rows}) {
        $self->{_rows} = $self->all->get;
    }

    $self->{_pos} //= 0;

    return undef if $self->{_pos} >= @{$self->{_rows}};

    return $self->{_rows}[$self->{_pos}++];
}

=head2 prefetch

    my $rs_with_prefetch = $rs->prefetch('related_table');

Adds a prefetch clause to the result set for eager loading of related data.

=over 4

=item B<Parameters>

=over 8

=item C<$prefetch>

Prefetch specification (string or arrayref).

=back

=item B<Returns>

A new result set object with the prefetch clause added.

=item B<Notes>

This method returns a clone of the result set and does not modify the original.

=back

=cut

sub prefetch {
    my ($self, $prefetch) = @_;

    my $clone = bless {
        %$self,
        _attrs => {
            %{$self->{_attrs}},
            prefetch => $prefetch,
        },
    }, ref $self;

    return $clone;
}

=head2 reset

    $rs->reset;
    # Now $rs->next will start from the first row again

Resets the internal iterator position.

=over 4

=item B<Returns>

The result set object itself (for chaining).

=back

=cut

sub reset {
    my $self = shift;
    $self->{_pos} = 0;
    return $self;
}

=head2 search

    my $filtered_rs = $rs->search({ active => 1 }, { order_by => 'name' });

Adds search conditions and attributes to the result set.

=over 4

=item B<Parameters>

=over 8

=item C<$cond>

Hash reference of search conditions (optional).

=item C<$attrs>

Hash reference of search attributes like order_by, rows, etc. (optional).

=back

=item B<Returns>

A new result set object with the combined conditions and attributes.

=item B<Notes>

This method returns a clone of the result set. Conditions and attributes
are merged with any existing ones from the original result set.

=back

=cut

sub search {
    my ($self, $cond, $attrs) = @_;

    my $clone = bless {
        %$self,
        _cond  => { %{$self->{_cond}},  %{$cond  || {}} },
        _attrs => { %{$self->{_attrs}}, %{$attrs || {}} },
        _rows  => undef,  # Reset cached results
        _pos   => 0,
    }, ref $self;

    return $clone;
}

=head2 single

    my $row = $rs->single;
    # Returns first row (blocking), or undef

Returns the first row from the result set (blocking version).

=over 4

=item B<Returns>

A L<DBIx::Class::Async::Row> object, or C<undef> if no rows match.

=item B<Notes>

This method performs a blocking fetch. For non-blocking operation, use
C<first> or C<single_future>.

=back

=cut

sub single {
    my $self = shift;

    return $self->search(undef, { rows => 1 })->next;
}

=head2 single_future

    $rs->single_future->then(sub {
        my ($row) = @_;
        if ($row) {
            # Process single row
        }
    });

Returns a single row matching the current search criteria (non-blocking).

=over 4

=item B<Returns>

A L<Future> that resolves to a L<DBIx::Class::Async::Row> object if found,
or C<undef> if not found.

=item B<Notes>

For simple primary key lookups, this method optimizes by using C<find>
internally. For complex queries, it adds C<rows =E<gt> 1> to the search
attributes.

=back

=cut

sub single_future {
    my $self = shift;

    # Check if this is a simple primary key lookup
    my @pk = $self->result_source->primary_columns;
    if (@pk == 1
        && keys %{$self->{_cond}} == 1
        && exists $self->{_cond}{$pk[0]}
        && !ref $self->{_cond}{$pk[0]}) {

        # Use find() for simple PK lookups - goes directly to worker
        return $self->{async_db}->find(
            $self->{source_name},
            $self->{_cond}{$pk[0]}
        )->then(sub {
            my $data = shift;

            # If no row found, return undef
            return Future->done(undef) unless $data;

            my $row = DBIx::Class::Async::Row->new(
                schema      => $self->{schema},
                async_db    => $self->{async_db},
                source_name => $self->{source_name},
                row_data    => $data,
            );

            return Future->done($row);
        });
    }

    # For complex queries, use search with limit
    my $attrs = {
        %{$self->{_attrs}},
        rows => 1,
    };

    return $self->{async_db}->search(
        $self->{source_name},
        $self->{_cond},
        $attrs
    )->then(sub {
        my $rows = shift;
        my $data = (ref $rows eq 'ARRAY') ? $rows->[0] : $rows;

        # If no row found, return undef
        return Future->done(undef) unless $data;

        my $row = DBIx::Class::Async::Row->new(
            schema      => $self->{schema},
            async_db    => $self->{async_db},
            source_name => $self->{source_name},
            row_data    => $data,
        );

        return Future->done($row);
    });
}

=head2 update

    $rs->search({ status => 'pending' })->update({ status => 'processed' })
       ->then(sub {
           my ($rows_affected) = @_;
           say "Updated $rows_affected rows";
       });

Updates all rows matching the current search criteria.

=over 4

=item B<Parameters>

=over 8

=item C<$data>

Hash reference containing column-value pairs to update.

=back

=item B<Returns>

A L<Future> that resolves to the number of rows affected.

=item B<Notes>

This performs a bulk update using the search conditions. For individual
row updates, use C<update> on the row object instead.

=back

=cut

sub update {
    my ($self, $data) = @_;

    # Perform a single bulk update via the worker
    # This uses the search condition (e.g., { active => 1 })
    # instead of individual row IDs.
    return $self->{async_db}->update_bulk(
        $self->{source_name},
        $self->{_cond} || {},
        $data
    )->then(sub {
        my ($rows_affected) = @_;
        return Future->done($rows_affected);
    });
}

=head2 as_query

    my ($cond, $attrs) = $rs->as_query;

Returns the internal search conditions and attributes.

=over 4

=item B<Returns>

A list containing two hash references: conditions and attributes.

=back

=cut

sub as_query {
    my $self = shift;
    return ($self->{_cond}, $self->{_attrs});
}

=head2 result_source

    my $source = $rs->result_source;

Returns the result source object for this result set.

=over 4

=item B<Returns>

A L<DBIx::Class::ResultSource> object.

=back

=cut

sub result_source {
    my $self = shift;
    return $self->_get_source;
}

=head2 source

    my $source_name = $rs->source_name;

Returns the source name for this result set.

=over 4

=item B<Returns>

The source name (string).

=back

=cut

sub source_name {
    my $self = shift;
    return $self->{source_name};
}

=head2 source

    my $source = $rs->source;

Alias for C<result_source>.

=over 4

=item B<Returns>

A L<DBIx::Class::ResultSource> object.

=back

=cut

sub source { shift->result_source(@_) }

=head1 CHAINABLE MODIFIERS

The following methods return a new result set with the specified attribute
added or modified:

=over 4

=item C<rows($number)> - Limits the number of rows returned

=item C<page($number)> - Specifies the page number for pagination

=item C<order_by($spec)> - Specifies the sort order

=item C<columns($spec)> - Specifies which columns to select

=item C<group_by($spec)> - Specifies GROUP BY clause

=item C<having($spec)> - Specifies HAVING clause

=item C<distinct($bool)> - Specifies DISTINCT modifier

=back

Example:

    my $paginated = $rs->rows(10)->page(2)->order_by('created_at DESC');

These methods do not modify the original result set and do not execute any
database queries.

=head1 INTERNAL METHODS

These methods are for internal use and are documented for completeness.

=head2 _get_source

    my $source = $rs->_get_source;

Returns the result source object, loading it lazily if needed.

=head2 search_future

    $rs->search_future->then(sub {
        # Same as all_future, alias for API consistency
    });

Alias for C<all_future>.

=head2 first_future

    $rs->first_future->then(sub {
        # Same as single_future, alias for API consistency
    });

Alias for C<single_future>.

=cut

# Internal methods and dynamic method generation follow...

sub _get_source {
    my $self = shift;

    unless ($self->{_source}) {
        $self->{_source} = $self->{schema}->source($self->{source_name});
    }

    return $self->{_source};
}

# Alias for convenience
sub search_future { shift->all_future(@_)    }
sub first_future  { shift->single_future(@_) }

# Chainable modifiers
for my $method (qw(rows page order_by columns group_by having distinct)) {
    eval qq{
        sub $method {
            my (\$self, \$value) = \@_;

            my \$clone = bless {
                %\$self,
                _attrs => {
                    %{\$self->{_attrs}},
                    $method => \$value,
                },
            }, ref \$self;

            return \$clone;
        }
    };
}

=head1 SEE ALSO

=over 4

=item *

L<DBIx::Class::Async> - Asynchronous DBIx::Class interface

=item *

L<DBIx::Class::ResultSet> - Synchronous DBIx::Class result set interface

=item *

L<DBIx::Class::Async::Row> - Asynchronous row objects

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

    perldoc DBIx::Class::Async::ResultSet

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

1; # End of DBIx::Class::Async::ResultSet
