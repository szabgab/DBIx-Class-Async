
use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw( tempfile );
use JSON::MaybeXS;

# Load the Proxy Schema
use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

my $loop = IO::Async::Loop->new;
my $json = JSON::MaybeXS->new->utf8->canonical;

# 1. Setup Temporary SQLite Database file
my ($fh, $filename) = tempfile( UNLINK => 1, SUFFIX => '.db' );
my $dsn = "dbi:SQLite:dbname=$filename";

# 2. Initialize the Async Proxy
my $async_schema = DBIx::Class::Async::Schema->connect(
    $dsn,
    undef, undef,
    {
        schema_class => 'TestSchema',
        loop         => $loop,
        workers      => 2,
    }
);

# 3. Deploy the schema
# We use the native schema side for the deploy (Synchronous)
$async_schema->{_native_schema}->deploy();

# --- FIX 1: Ensure workers are synced and the DB file is ready ---
$async_schema->sync_metadata->get;

# 4. Insert initial data via the native schema
$async_schema->{_native_schema}->resultset('Product')->create({
    name     => 'Gaming Mouse',
    sku      => 'MOUSE-99',
    price    => 49.99,
    metadata => '{"color":"rgb","dpi":16000}'
});

# 5. WARM UP: Ensure workers are alive
$async_schema->resultset('Product')->count->get;

# 6. LATE REGISTRATION: Register inflation via the Proxy
$async_schema->inflate_column('Product', 'metadata', {
    inflate => sub {
        my $val = shift;
        return $val if ref $val; # <--- ADD THIS: Skip if already inflated (HASH)
        return $json->decode($val);
    },
    deflate => sub {
        my $val = shift;
        return $val unless ref $val; # <--- ADD THIS: Skip if already deflated (STRING)
        return $json->encode($val);
    },
});

# 7. EXECUTE TEST: Find and Modify via Async
# --- FIX 2: Corrected the logic and removed the duplicate 'my $test_f' ---
my $test_f = $async_schema->resultset('Product')
    ->find({ sku => 'MOUSE-99' })
    ->then(sub {
        my $product = shift;

        is(ref($product->metadata), 'HASH', 'Inflated to HASH successfully');

        my $meta = $product->metadata;
        $meta->{dpi} = 25000;

        # We return the update future to chain it
        return $async_schema->resultset('Product')->update(
            { sku => 'MOUSE-99' },
            { metadata => $meta }
        );
    });

# Block until the chain (Find + Update) is fully complete
$test_f->get;

# 8. VERIFY: Check raw value in the file
# We use a fresh DBH handle to ensure we aren't seeing cached data
my $raw_val = $async_schema->{_native_schema}->storage->dbh->selectrow_array(
    "SELECT metadata FROM products WHERE sku = 'MOUSE-99'"
);

is($raw_val, '{"color":"rgb","dpi":25000}', 'Round-trip successful through worker processes');

done_testing();
