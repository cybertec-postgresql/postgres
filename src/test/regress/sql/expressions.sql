--
-- expression evaluation tests that don't fit into a more specific file
--

--
-- Tests for SQLVAlueFunction
--


-- current_date  (always matches because of transactional behaviour)
SELECT date(now())::text = current_date::text;


-- current_time / localtime
SELECT now()::timetz::text = current_time::text;
SELECT now()::timetz(4)::text = current_time(4)::text;
SELECT now()::time::text = localtime::text;
SELECT now()::time(3)::text = localtime(3)::text;

-- current_timestamp / localtimestamp (always matches because of transactional behaviour)
SELECT current_timestamp = NOW();
-- precision
SELECT length(current_timestamp::text) >= length(current_timestamp(0)::text);
-- localtimestamp
SELECT now()::timestamp::text = localtimestamp::text;

-- current_role/user/user is tested in rolnames.sql

-- current database / catalog
SELECT current_catalog = current_database();

-- current_schema
SELECT current_schema;
SET search_path = 'notme';
SELECT current_schema;
SET search_path = 'pg_catalog';
SELECT current_schema;
RESET search_path;


--
-- Tests for BETWEEN
--

explain (costs off)
select count(*) from date_tbl
  where f1 between '1997-01-01' and '1998-01-01';
select count(*) from date_tbl
  where f1 between '1997-01-01' and '1998-01-01';

explain (costs off)
select count(*) from date_tbl
  where f1 not between '1997-01-01' and '1998-01-01';
select count(*) from date_tbl
  where f1 not between '1997-01-01' and '1998-01-01';

explain (costs off)
select count(*) from date_tbl
  where f1 between symmetric '1997-01-01' and '1998-01-01';
select count(*) from date_tbl
  where f1 between symmetric '1997-01-01' and '1998-01-01';

explain (costs off)
select count(*) from date_tbl
  where f1 not between symmetric '1997-01-01' and '1998-01-01';
select count(*) from date_tbl
  where f1 not between symmetric '1997-01-01' and '1998-01-01';

--
-- Tests for ScalarArrayOpExpr with a hashfn
--

-- create a stable function so that the tests below are not
-- evaluated using the planner's constant folding.
begin;

create function return_int_input(int) returns int as $$
begin
	return $1;
end;
$$ language plpgsql stable;

create function return_text_input(text) returns text as $$
begin
	return $1;
end;
$$ language plpgsql stable;

select return_int_input(1) in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1);
select return_int_input(1) in (10, 9, 2, 8, 3, 7, 4, 6, 5, null);
select return_int_input(1) in (null, null, null, null, null, null, null, null, null, null, null);
select return_int_input(1) in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1, null);
select return_int_input(null::int) in (10, 9, 2, 8, 3, 7, 4, 6, 5, 1);
select return_int_input(null::int) in (10, 9, 2, 8, 3, 7, 4, 6, 5, null);
select return_text_input('a') in ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j');

rollback;

-- Test with non-strict equality function.
-- We need to create our own type for this.

begin;

create type myint;
create function myintin(cstring) returns myint strict immutable language
  internal as 'int4in';
create function myintout(myint) returns cstring strict immutable language
  internal as 'int4out';
create function myinthash(myint) returns integer strict immutable language
  internal as 'hashint4';

create type myint (input = myintin, output = myintout, like = int4);

create cast (int4 as myint) without function;
create cast (myint as int4) without function;

create function myinteq(myint, myint) returns bool as $$
begin
  if $1 is null and $2 is null then
    return true;
  else
    return $1::int = $2::int;
  end if;
end;
$$ language plpgsql immutable;

create operator = (
  leftarg    = myint,
  rightarg   = myint,
  commutator = =,
  negator    = <>,
  procedure  = myinteq,
  restrict   = eqsel,
  join       = eqjoinsel,
  merges
);

create operator class myint_ops
default for type myint using hash as
  operator    1   =  (myint, myint),
  function    1   myinthash(myint);

create table inttest (a myint);
insert into inttest values(1::myint),(null);

-- try an array with enough elements to cause hashing
select * from inttest where a in (1::myint,2::myint,3::myint,4::myint,5::myint,6::myint,7::myint,8::myint,9::myint, null);
-- ensure the result matched with the non-hashed version.  We simply remove
-- some array elements so that we don't reach the hashing threshold.
select * from inttest where a in (1::myint,2::myint,3::myint,4::myint,5::myint, null);

rollback;
