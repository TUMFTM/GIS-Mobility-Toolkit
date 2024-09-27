SET search_path TO $schema;
CREATE
OR REPLACE FUNCTION jsonb_merge_deep (jsonb, jsonb) RETURNS jsonb LANGUAGE SQL IMMUTABLE AS $func$
  select case jsonb_typeof($1)
    when 'object' then case jsonb_typeof($2)
      when 'object' then (
        select    jsonb_object_agg(k, case
                    when e2.v is null then e1.v
                    when e1.v is null then e2.v
                    else jsonb_merge_deep(e1.v, e2.v)
                  end)
        from      jsonb_each($1) e1(k, v)
        full join jsonb_each($2) e2(k, v) using (k)
      )
      else $2
    end
    when 'array' then $1 || $2
    else $2
  end
$func$;

CREATE
OR REPLACE FUNCTION jsonb_merge_deep (_jsons jsonb[]) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $func$
DECLARE 
	_json jsonb;
	retn jsonb := '{}'::jsonb;
BEGIN
   FOREACH _json IN ARRAY _jsons
   LOOP
      retn = jsonb_merge_deep(retn, _json);
   END LOOP;
   RETURN retn;
END
$func$;

CREATE
OR REPLACE FUNCTION hex_to_int (hexval VARCHAR) RETURNS INTEGER AS $$
DECLARE
    result  int;
BEGIN
    EXECUTE 'SELECT x' || quote_literal(hexval) || '::int' INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION array_distinct (anyarray) RETURNS anyarray AS $f$
  SELECT array_agg(DISTINCT x) FROM unnest($1) t(x);
$f$ LANGUAGE SQL IMMUTABLE;

CREATE
OR REPLACE FUNCTION area_agg (FLOAT, FLOAT, FLOAT, INT) RETURNS FLOAT LANGUAGE SQL AS $$
    SELECT CASE 
	    WHEN $1 = -1.0
	    THEN LEAST($2/$4, $3) 
	    ELSE $1 + LEAST(($2-$1)/$4, $3) 
     END
$$;

CREATE
OR REPLACE AGGREGATE area_aggregator (FLOAT, FLOAT, INT) (sfunc = area_agg, stype = FLOAT, initcond = -1.0);