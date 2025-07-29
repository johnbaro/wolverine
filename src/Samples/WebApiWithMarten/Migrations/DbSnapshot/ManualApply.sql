create table mt_doc_order
(
    id               uuid                                                                                          not null
        constraint pkey_mt_doc_order_id
            primary key,
    data             jsonb                                                                                         not null,
    mt_last_modified timestamp with time zone default transaction_timestamp(),
    mt_version       uuid                     default (md5(((random())::text || (clock_timestamp())::text)))::uuid not null,
    mt_dotnet_type   varchar
);

alter table mt_doc_order
    owner to postgres;

create function mt_immutable_timestamp(value text) returns timestamp without time zone
    immutable
    language sql
as
$$
select value::timestamp

$$;

alter function mt_immutable_timestamp(text) owner to postgres;

create function mt_immutable_timestamptz(value text) returns timestamp with time zone
    immutable
    language sql
as
$$
select value::timestamptz

$$;

alter function mt_immutable_timestamptz(text) owner to postgres;

create function mt_immutable_time(value text) returns time without time zone
    immutable
    language sql
as
$$
select value::time

$$;

alter function mt_immutable_time(text) owner to postgres;

create function mt_immutable_date(value text) returns date
    immutable
    language sql
as
$$
select value::date

$$;

alter function mt_immutable_date(text) owner to postgres;

create function mt_grams_vector(text, use_unaccent boolean DEFAULT false) returns tsvector
    immutable
    strict
    language plpgsql
as
$$
BEGIN
RETURN (SELECT array_to_string(orders.mt_grams_array($1, use_unaccent), ' ') ::tsvector);
END
$$;

alter function mt_grams_vector(text, boolean) owner to postgres;

create function mt_grams_query(text, use_unaccent boolean DEFAULT false) returns tsquery
    immutable
    strict
    language plpgsql
as
$$
BEGIN
RETURN (SELECT array_to_string(orders.mt_grams_array($1, use_unaccent), ' & ') ::tsquery);
END
$$;

alter function mt_grams_query(text, boolean) owner to postgres;

create function mt_grams_array(words text, use_unaccent boolean DEFAULT false) returns text[]
    immutable
    strict
    language plpgsql
as
$$
DECLARE
result text[];
        DECLARE
word text;
        DECLARE
clean_word text;
BEGIN
                FOREACH
word IN ARRAY string_to_array(words, ' ')
                LOOP
                     clean_word = regexp_replace(orders.mt_safe_unaccent(use_unaccent, word), '[^a-zA-Z0-9]+', '','g');
FOR i IN 1 .. length(clean_word)
                     LOOP
                         result := result || quote_literal(substr(lower(clean_word), i, 1));
                         result
:= result || quote_literal(substr(lower(clean_word), i, 2));
                         result
:= result || quote_literal(substr(lower(clean_word), i, 3));
END LOOP;
END LOOP;

RETURN ARRAY(SELECT DISTINCT e FROM unnest(result) AS a(e) ORDER BY e);
END;
$$;

alter function mt_grams_array(text, boolean) owner to postgres;

create function mt_jsonb_append(jsonb, text[], jsonb, boolean, jsonb DEFAULT NULL::jsonb) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    location ALIAS FOR $2;
    val ALIAS FOR $3;
    if_not_exists ALIAS FOR $4;
    patch_expression ALIAS FOR $5;
    tmp_value jsonb;
BEGIN
    tmp_value = retval #> location;
    IF tmp_value IS NOT NULL AND jsonb_typeof(tmp_value) = 'array' THEN
        CASE
            WHEN NOT if_not_exists THEN
                retval = jsonb_set(retval, location, tmp_value || val, FALSE);
WHEN patch_expression IS NULL AND jsonb_typeof(val) = 'object' AND NOT tmp_value @> jsonb_build_array(val) THEN
                retval = jsonb_set(retval, location, tmp_value || val, FALSE);
WHEN patch_expression IS NULL AND jsonb_typeof(val) <> 'object' AND NOT tmp_value @> val THEN
                retval = jsonb_set(retval, location, tmp_value || val, FALSE);
WHEN patch_expression IS NOT NULL AND jsonb_typeof(patch_expression) = 'array' AND jsonb_array_length(patch_expression) = 0 THEN
                retval = jsonb_set(retval, location, tmp_value || val, FALSE);
ELSE NULL;
END CASE;
END IF;
RETURN retval;
END;
$$;

alter function mt_jsonb_append(jsonb, text[], jsonb, boolean, jsonb) owner to postgres;

create function mt_jsonb_copy(jsonb, text[], text[]) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    src_path ALIAS FOR $2;
    dst_path ALIAS FOR $3;
    tmp_value jsonb;
BEGIN
    tmp_value = retval #> src_path;
    retval = orders.mt_jsonb_fix_null_parent(retval, dst_path);
RETURN jsonb_set(retval, dst_path, tmp_value::jsonb, TRUE);
END;
$$;

alter function mt_jsonb_copy(jsonb, text[], text[]) owner to postgres;

create function mt_jsonb_duplicate(jsonb, text[], jsonb) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    location ALIAS FOR $2;
    targets ALIAS FOR $3;
    tmp_value jsonb;
    target_path text[];
    target text;
BEGIN
FOR target IN SELECT jsonb_array_elements_text(targets)
                         LOOP
                  target_path = orders.mt_jsonb_path_to_array(target, '\.');
retval = orders.mt_jsonb_copy(retval, location, target_path);
END LOOP;

RETURN retval;
END;
$$;

alter function mt_jsonb_duplicate(jsonb, text[], jsonb) owner to postgres;

create function mt_jsonb_fix_null_parent(jsonb, text[]) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    dst_path ALIAS FOR $2;
    dst_path_segment text[] = ARRAY[]::text[];
    dst_path_array_length integer;
    i integer = 1;
BEGIN
    dst_path_array_length = array_length(dst_path, 1);
    WHILE i <=(dst_path_array_length - 1)
    LOOP
        dst_path_segment = dst_path_segment || ARRAY[dst_path[i]];
        IF retval #> dst_path_segment IS NULL OR retval #> dst_path_segment = 'null'::jsonb THEN
            retval = jsonb_set(retval, dst_path_segment, '{}'::jsonb, TRUE);
END IF;
        i = i + 1;
END LOOP;

RETURN retval;
END;
$$;

alter function mt_jsonb_fix_null_parent(jsonb, text[]) owner to postgres;

create function mt_jsonb_increment(jsonb, text[], numeric) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    location ALIAS FOR $2;
    increment_value ALIAS FOR $3;
    tmp_value jsonb;
BEGIN
    tmp_value = retval #> location;
    IF tmp_value IS NULL THEN
        tmp_value = to_jsonb(0);
END IF;

RETURN jsonb_set(retval, location, to_jsonb(tmp_value::numeric + increment_value), TRUE);
END;
$$;

alter function mt_jsonb_increment(jsonb, text[], numeric) owner to postgres;

create function mt_jsonb_insert(jsonb, text[], jsonb, integer, boolean, jsonb DEFAULT NULL::jsonb) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    location ALIAS FOR $2;
    val ALIAS FOR $3;
    elm_index ALIAS FOR $4;
    if_not_exists ALIAS FOR $5;
    patch_expression ALIAS FOR $6;
    tmp_value jsonb;
BEGIN
    tmp_value = retval #> location;
    IF tmp_value IS NOT NULL AND jsonb_typeof(tmp_value) = 'array' THEN
        IF elm_index IS NULL THEN
            elm_index = jsonb_array_length(tmp_value) + 1;
END IF;
CASE
            WHEN NOT if_not_exists THEN
                retval = jsonb_insert(retval, location || elm_index::text, val);
WHEN patch_expression IS NULL AND jsonb_typeof(val) = 'object' AND NOT tmp_value @> jsonb_build_array(val) THEN
                retval = jsonb_insert(retval, location || elm_index::text, val);
WHEN patch_expression IS NULL AND jsonb_typeof(val) <> 'object' AND NOT tmp_value @> val THEN
                retval = jsonb_insert(retval, location || elm_index::text, val);
WHEN patch_expression IS NOT NULL AND jsonb_typeof(patch_expression) = 'array' AND jsonb_array_length(patch_expression) = 0 THEN
                retval = jsonb_insert(retval, location || elm_index::text, val);
ELSE NULL;
END CASE;
END IF;
RETURN retval;
END;
$$;

alter function mt_jsonb_insert(jsonb, text[], jsonb, integer, boolean, jsonb) owner to postgres;

create function mt_jsonb_move(jsonb, text[], text) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    src_path ALIAS FOR $2;
    dst_name ALIAS FOR $3;
    dst_path text[];
    tmp_value jsonb;
BEGIN
    tmp_value = retval #> src_path;
    retval = retval #- src_path;
    dst_path = src_path;
    dst_path[array_length(dst_path, 1)] = dst_name;
    retval = orders.mt_jsonb_fix_null_parent(retval, dst_path);
RETURN jsonb_set(retval, dst_path, tmp_value, TRUE);
END;
$$;

alter function mt_jsonb_move(jsonb, text[], text) owner to postgres;

create function mt_jsonb_path_to_array(text, character) returns text[]
    language plpgsql
as
$$
DECLARE
location ALIAS FOR $1;
    regex_pattern ALIAS FOR $2;
BEGIN
RETURN regexp_split_to_array(location, regex_pattern)::text[];
END;
$$;

alter function mt_jsonb_path_to_array(text, char) owner to postgres;

create function mt_jsonb_remove(jsonb, text[], jsonb) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    location ALIAS FOR $2;
    val ALIAS FOR $3;
    tmp_value jsonb;
    tmp_remove jsonb;
    patch_remove jsonb;
BEGIN
    tmp_value = retval #> location;
    IF tmp_value IS NOT NULL AND jsonb_typeof(tmp_value) = 'array' THEN
        IF jsonb_typeof(val) = 'array' THEN
            tmp_remove = val;
ELSE
            tmp_remove = jsonb_build_array(val);
END IF;

FOR patch_remove IN SELECT * FROM jsonb_array_elements(tmp_remove)
                                      LOOP
    tmp_value =(SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(tmp_value) AS elem
            WHERE elem <> patch_remove);
END LOOP;

        IF tmp_value IS NULL THEN
            tmp_value = '[]'::jsonb;
END IF;
END IF;
RETURN jsonb_set(retval, location, tmp_value, FALSE);
END;
$$;

alter function mt_jsonb_remove(jsonb, text[], jsonb) owner to postgres;

create function mt_jsonb_patch(jsonb, jsonb) returns jsonb
    language plpgsql
as
$$
DECLARE
retval ALIAS FOR $1;
    patchset ALIAS FOR $2;
    patch jsonb;
    patch_path text[];
    patch_expression jsonb;
value jsonb;
BEGIN
FOR patch IN SELECT * from jsonb_array_elements(patchset)
                               LOOP
    patch_path = orders.mt_jsonb_path_to_array((patch->>'path')::text, '\.');

patch_expression = null;
        IF (patch->>'type') IN ('remove', 'append_if_not_exists', 'insert_if_not_exists') AND (patch->>'expression') IS NOT NULL THEN
            patch_expression = jsonb_path_query_array(retval #> patch_path, (patch->>'expression')::jsonpath);
END IF;

CASE patch->>'type'
            WHEN 'set' THEN
                retval = jsonb_set(retval, patch_path, (patch->'value')::jsonb, TRUE);
WHEN 'delete' THEN
                retval = retval#-patch_path;
WHEN 'append' THEN
                retval = orders.mt_jsonb_append(retval, patch_path, (patch->'value')::jsonb, FALSE);
WHEN 'append_if_not_exists' THEN
                retval = orders.mt_jsonb_append(retval, patch_path, (patch->'value')::jsonb, TRUE, patch_expression);
WHEN 'insert' THEN
                retval = orders.mt_jsonb_insert(retval, patch_path, (patch->'value')::jsonb, (patch->>'index')::integer, FALSE);
WHEN 'insert_if_not_exists' THEN
                retval = orders.mt_jsonb_insert(retval, patch_path, (patch->'value')::jsonb, (patch->>'index')::integer, TRUE, patch_expression);
WHEN 'remove' THEN
                retval = orders.mt_jsonb_remove(retval, patch_path, COALESCE(patch_expression, (patch->'value')::jsonb));
WHEN 'duplicate' THEN
                retval = orders.mt_jsonb_duplicate(retval, patch_path, (patch->'targets')::jsonb);
WHEN 'rename' THEN
                retval = orders.mt_jsonb_move(retval, patch_path, (patch->>'to')::text);
WHEN 'increment' THEN
                retval = orders.mt_jsonb_increment(retval, patch_path, (patch->>'increment')::numeric);
WHEN 'increment_float' THEN
                retval = orders.mt_jsonb_increment(retval, patch_path, (patch->>'increment')::numeric);
ELSE NULL;
END CASE;
END LOOP;
RETURN retval;
END;
$$;

alter function mt_jsonb_patch(jsonb, jsonb) owner to postgres;

create function mt_safe_unaccent(use_unaccent boolean, word text) returns text
    immutable
    strict
    language plpgsql
as
$$
BEGIN
IF use_unaccent THEN
    RETURN unaccent(word);
ELSE
    RETURN word;
END IF;
END;
$$;

alter function mt_safe_unaccent(boolean, text) owner to postgres;

create function mt_upsert_order(doc jsonb, docdotnettype character varying, docid uuid, docversion uuid) returns uuid
    language plpgsql
as
$$
DECLARE
final_version uuid;
BEGIN
INSERT INTO orders.mt_doc_order ("data", "mt_dotnet_type", "id", "mt_version", mt_last_modified) VALUES (doc, docDotNetType, docId, docVersion, transaction_timestamp())
    ON CONFLICT (id)
  DO UPDATE SET "data" = doc, "mt_dotnet_type" = docDotNetType, "mt_version" = docVersion, mt_last_modified = transaction_timestamp();

SELECT mt_version FROM orders.mt_doc_order into final_version WHERE id = docId ;
RETURN final_version;
END;
$$;

alter function mt_upsert_order(jsonb, varchar, uuid, uuid) owner to postgres;

create function mt_insert_order(doc jsonb, docdotnettype character varying, docid uuid, docversion uuid) returns uuid
    language plpgsql
as
$$
BEGIN
INSERT INTO orders.mt_doc_order ("data", "mt_dotnet_type", "id", "mt_version", mt_last_modified) VALUES (doc, docDotNetType, docId, docVersion, transaction_timestamp());

RETURN docVersion;
END;
$$;

alter function mt_insert_order(jsonb, varchar, uuid, uuid) owner to postgres;

create function mt_update_order(doc jsonb, docdotnettype character varying, docid uuid, docversion uuid) returns uuid
    language plpgsql
as
$$
DECLARE
final_version uuid;
BEGIN
UPDATE orders.mt_doc_order SET "data" = doc, "mt_dotnet_type" = docDotNetType, "mt_version" = docVersion, mt_last_modified = transaction_timestamp() where id = docId;

SELECT mt_version FROM orders.mt_doc_order into final_version WHERE id = docId ;
RETURN final_version;
END;
$$;

alter function mt_update_order(jsonb, varchar, uuid, uuid) owner to postgres;

