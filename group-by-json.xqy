xquery version "1.0-ml";

(:~
 : evaluate map / JSON serialized cts:group-by query definitions
 :
 : @author Joe Bryan
 : @version 0.1
 :)
module namespace grpj = "http://marklogic.com/cts/group-by/json";

import module namespace cts = "http://marklogic.com/cts" at "./group-by.xqy";
import module namespace ctx = "http://marklogic.com/cts-extensions"
  at "/mlpm_modules/cts-extensions/cts-extensions.xqy";

declare option xdmp:mapping "false";

(: get value from a map by key, converting json:arrays to sequences :)
declare %private function grpj:get-values($map, $key)
{
  let $obj := map:get($map, $key)
  return
    if ($obj instance of json:array)
    then json:array-values($obj)
    else $obj
};

(:~
 : construct cts:row, cts:column, and cts:compute objects from map / JSON objects
 :
 : @param $map as `map:map` or `json:object`
 :)
declare %private function grpj:query-parser(
  $query,
  $name as xs:string,
  $fn as function(*)
) as function(*)*
{
  for $entry in grpj:get-values($query, $name)
  let $ref := (map:get($entry, "ref"), $entry)[1]
  let $alias := (map:get($entry, "alias"), map:get($ref, "localname"), map:get($ref, "path-expression"))[1]
  return
    if ($name eq "computes" and fn:function-arity($fn) eq 3)
    then
      let $aggregate-fn := map:get($entry, "fn")
      let $alias := (map:get($entry, "alias"), $alias || "-" || $aggregate-fn)[1]
      return
        $fn( $alias, $aggregate-fn, ctx:reference-from-map($ref) )
    else
      $fn( $alias, ctx:reference-from-map($ref) )
};

(:~
 : evaluate map / JSON serialized cts:group-by query definitions
 :
 : @param $query as `map:map` or `json:object`
 :)
declare function grpj:query($query)
{
  let $rows := grpj:query-parser($query, "rows", cts:row(?, ?))
  let $columns := grpj:query-parser($query, "columns", cts:column(?, ?))
  let $computes := grpj:query-parser($query, "computes", cts:compute(?, ?, ?))
  let $options := grpj:get-values($query, "options")
  let $result-type := (map:get($query, "result-type"), "group-by")[1]
  let $fn :=
    switch( $result-type )
    case "group-by" return cts:group-by(?, ?)
    case "cross-product" return cts:cross-product(?, ?)
    case "cube" return cts:cube(?, ?)
    default return fn:error(xs:QName("UNKOWN-FN"), "unknown group-by type: " || $result-type)
  return $fn( ($rows, $columns, $computes), $options )
};
