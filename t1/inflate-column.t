#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use File::Temp;
use JSON::MaybeXS;
use IO::Async::Loop;
use DBIx::Class::Async::Schema;

use lib 't/lib';

my $json           = JSON::MaybeXS->new->utf8->canonical;
my $loop           = IO::Async::Loop->new;
my ($fh, $db_file) = File::Temp::tempfile(SUFFIX => '.db', UNLINK => 1);
my $schema         = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file", undef, undef, {},
    { workers      => 2,
      schema_class => 'TestSchema',
      async_loop   => $loop,
      cache_ttl    => 60,
    },
);

# Deploy the schema
$schema->{_native_schema}->deploy;

# Ensure workers are synced and the DB file is ready
$schema->sync_metadata->get;

# Insert initial data via the native schema
$schema->{_native_schema}->resultset('Product')->create({
    name     => 'Gaming Mouse',
    sku      => 'MOUSE-99',
    price    => 49.99,
    metadata => '{"color":"rgb","dpi":16000}'
});

# Ensure workers are alive
$schema->resultset('Product')->count->get;

# Register inflation via the proxy
$schema->inflate_column('Product', 'metadata', {
    inflate => sub {
        my $value = shift;
        return $value if ref $value;
        return $json->decode($value);
    },
    deflate => sub {
        my $value = shift;
        return $value unless ref $value;
        return $json->encode($value);
    },
});

# Find and modify via async
$schema->resultset('Product')
       ->find({ sku => 'MOUSE-99' })
       ->then(sub {
            my $product = shift;

            is(ref($product->metadata), 'HASH', 'Inflated to HASH successfully');

            my $meta = $product->metadata;
            $meta->{dpi} = 25000;

            return $schema->resultset('Product')
                          ->update(
                            { sku      => 'MOUSE-99' },
                            { metadata => $meta      });
       })->get;


# Check raw value in the file
my $raw_value = $schema->{_native_schema}->storage->dbh->selectrow_array(
    "SELECT metadata FROM products WHERE sku = 'MOUSE-99'"
);

is($raw_value, '{"color":"rgb","dpi":25000}', 'Round-trip successful through worker processes');

done_testing;
