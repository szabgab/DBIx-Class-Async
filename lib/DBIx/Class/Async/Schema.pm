package DBIx::Class::Async::Schema;

use strict;
use warnings;
use utf8;

use Carp;
use Future;
use Try::Tiny;
use Scalar::Util 'blessed';
use DBIx::Class::Async;
use DBIx::Class::Async::Storage;
use DBIx::Class::Async::ResultSet;
use DBIx::Class::Async::Storage::DBI;
use Data::Dumper;


sub connect {
    my ($class, @args) = @_;

    # Separate async options from connect_info
    my $async_options = {};
    if (ref $args[-1] eq 'HASH' && !exists $args[-1]->{RaiseError}) {
        $async_options = pop @args;
    }

    my $schema_class = $async_options->{schema_class}
       or croak "schema_class is required in async options";

    my $schema_loaded = 0;
    if (eval { $schema_class->can('connect') }) {
        $schema_loaded = 1;
    }
    elsif (eval "require $schema_class") {
        $schema_loaded = 1;
    }
    elsif (eval "package main; \$${schema_class}::VERSION ||= '0.01'; 1") {
        $schema_loaded = 1;
    }

    unless ($schema_loaded) {
        croak "Cannot load schema class $schema_class: $@";
    }

    my $async_db = eval {
        DBIx::Class::Async->create_async_db(
            schema_class => $schema_class,
            connect_info => \@args,
            %$async_options,
        );
    };

    if ($@) {
        croak "Failed to create async engine: $@";
    }

    my $self = bless {
        _async_db      => $async_db,
        _sources_cache => {},
    }, $class;

    my $storage = DBIx::Class::Async::Storage::DBI->new(
        schema   => $self,
        async_db => $async_db,
    );

    $self->{_storage} = $storage;

    return $self;
}

sub storage {
    my $self = shift;
    return $self->{_storage};
}

sub resultset {
    my ($self, $source_name) = @_;

    croak "resultset() requires a source name" unless $source_name;

    return DBIx::Class::Async::ResultSet->new(
        schema          => $self->{_async_db}->{_schema_class},
        schema_instance => $self,
        async_db        => $self->{_async_db},
        source_name     => $source_name,
    );
}

sub source {
    my ($self, $source_name) = @_;

    unless (exists $self->{_sources_cache}{$source_name}) {
        my $schema_class = $self->{_async_db}->{_schema_class};
        my $connect_info = $self->{_async_db}->{_connect_info};
        my $temp_schema  = $schema_class->connect(@{$connect_info});
        $self->{_sources_cache}{$source_name} = $temp_schema->source($source_name);
        $temp_schema->storage->disconnect;
    }

    return $self->{sources_cache}{$source_name};
}


sub sources {
    my $self = shift;

    my $schema_class = $self->{_async_db}->{_schema_class};
    my $connect_info = $self->{_async_db}->{_connect_info};
    my $temp_schema = $schema_class->connect(@{$connect_info});
    my @sources = $temp_schema->sources;
    $temp_schema->storage->disconnect;

    return @sources;
}

sub AUTOLOAD {
    my $self = shift;

    return unless ref $self;

    our $AUTOLOAD;
    my ($method) = $AUTOLOAD =~ /([^:]+)$/;

    return if $method eq 'DESTROY';

    if ($self->{_async_db} && exists $self->{_async_db}->{schema}) {
        my $real_schema = $self->{_async_db}->{schema};
        if ($real_schema->can($method)) {
            return $real_schema->$method(@_);
        }
    }

    croak "Method $method not found in " . ref($self);
}

1; # End of DBIx::Class::Async::Schema
