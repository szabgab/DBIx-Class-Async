package DBIx::Class::Async::ResultSetColumn;

use strict;
use warnings;
use Future;

sub new {
    my ($class, %args) = @_;
    return bless {
        _resultset => $args{resultset},
        _column    => $args{column},
        _async_db  => $args{async_db},
    }, $class;
}

sub sum     { shift->_aggregate('sum') }
sub max     { shift->_aggregate('max') }
sub min     { shift->_aggregate('min') }
sub avg     { shift->_aggregate('avg') }
sub count   { shift->_aggregate('count') }
sub average { shift->_aggregate('average') }

sub _aggregate {
    my ($self, $func) = @_;
    my $db = $self->{_async_db};

    my $payload = $self->{_resultset}->_build_payload();

    $payload->{column} = $self->{_column};

    # We reuse the parent ResultSet's bridge and payload logic
    return DBIx::Class::Async::_call_worker(
        $db,
        $func,
        $payload,
    );
}

1;
