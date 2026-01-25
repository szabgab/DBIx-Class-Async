package DBIx::Class::Async::ResultComponent;

use strict;
use warnings;
use parent 'DBIx::Class::Row';
use Future;

sub update_future {
    my ($self, $upd) = @_;

    $self->set_inflated_columns($upd) if $upd;
    my %dirty = $self->get_dirty_columns;

    return Future->done($self) unless keys %dirty;

    return DBIx::Class::Async::ResultSet->new(
        schema      => $self->{_schema},
        async_db    => $self->{_async_db},
        source_name => $self->{_source_name},
        cond        => { $self->ident_condition },
    )->update(\%dirty)->then(sub {
        # Force the Future to resolve with $self (the Row object)
        # instead of the bridge's raw return value (usually 1 or 0E0)
        return Future->done($self);
    });
}

sub delete_future {
    my ($self) = @_;

    # Call 'delete' on our Async ResultSet
    return DBIx::Class::Async::ResultSet->new(
        schema      => $self->{_schema},
        async_db    => $self->{_async_db},
        source_name => $self->{_source_name},
        cond        => { $self->ident_condition },
    )->delete();
}

1;
