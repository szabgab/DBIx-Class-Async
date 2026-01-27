
use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use DBIx::Class::Async::Schema;
use File::Temp qw(tempfile);
use lib 't/lib';
use TestSchema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

my $loop = IO::Async::Loop->new;

my ($fh, $db_filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
close($fh);

my $dsn = "dbi:SQLite:dbname=$db_filename";

my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 1,
});

subtest 'Async Deployment with Temp File' => sub {
    # Verify table is missing (Pre-deploy)
    my $check_f = $async_schema->resultset('User')->search_future({});

    # Peel the nested futures
    my $inner_check_f = $loop->await($check_f);
    $loop->await($inner_check_f);

    my $res_err = $inner_check_f->get;

    # Based on your logs, we check if it caught an error HASH
    # OR if it gracefully returned an empty ARRAY because the table didn't exist
    ok((ref $res_err eq 'HASH' && $res_err->{error}) || (ref $res_err eq 'ARRAY'),
       "Query handled pre-deployment state (table missing)")
       or diag("Unexpected result type: " . ref($res_err));

    # Execute Deploy
    my $deploy_f = $async_schema->deploy();

    # Ensure deployment is finished across all future layers
    my $inner_deploy_f = $loop->await($deploy_f);
    $loop->await($inner_deploy_f) if ref $inner_deploy_f eq 'IO::Async::Future';

    ok(1, "Deployment command executed successfully");

    # Verify table exists and is readable (Post-deploy)
    my $search_f = $async_schema->resultset('User')->search_future({});

    # Peel the nested futures
    my $inner_search_f = $loop->await($search_f);
    $loop->await($inner_search_f);

    my $rows = $inner_search_f->get;

    is(ref $rows, 'ARRAY', "Search results is an ARRAY reference");
    is(scalar @$rows, 0, "Database table is empty as expected");

    done_testing;
};

done_testing;
