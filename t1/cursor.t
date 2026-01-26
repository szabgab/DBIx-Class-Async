#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
use Test::More;
use File::Temp qw(tempfile);
use IO::Async::Loop;
use lib 't/lib';
use TestSchema;
use DBIx::Class::Async::Schema;

BEGIN { $SIG{__WARN__} = sub {}; }

# 1. Setup real temporary SQLite database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);

my $dsn = "dbi:SQLite:dbname=$filename";
my $base_schema = TestSchema->connect($dsn);
$base_schema->deploy();

# 2. Initialize the Async Schema
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

# 3. Setup 5 records
$base_schema->resultset('User')->delete_all;
for (1..5) {
    $base_schema->resultset('User')
                ->create({ id => $_, name => "User $_" })
}

# 4. Get a cursor with batch size 2
my $rs = $async_schema->resultset('User')
                      ->search(undef, { order_by => 'id', rows => 2 });

my $cursor = $rs->cursor;

# Page 1, Row 1 (Hits DB)
my $r1 = $cursor->next->get;
is($r1->id, 1, 'First row correct');

# Page 1, Row 2 (From Buffer)
my $r2 = $cursor->next->get;
is($r2->id, 2, 'Second row from buffer');

# Page 2, Row 1 (Hits DB again for rows 3-4)
my $r3 = $cursor->next->get;
is($r3->id, 3, 'Third row (new batch) correct');

# Page 3, Row 1 (Hits DB for row 5)
$cursor->next->get; # skip 4
my $r5 = $cursor->next->get;
is($r5->id, 5, 'Final row correct');

done_testing;
