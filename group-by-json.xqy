xquery version "1.0-ml";

(:~
 : evaluate map / JSON serialized cts:group-by query definitions
 :
 : @author Joe Bryan
 :)
module namespace grpj = "http://marklogic.com/cts/group-by/json";

import module namespace cts = "http://marklogic.com/cts" at "./group-by.xqy";
import module namespace ctx = "http://marklogic.com/cts-extensions"
  at "/ext/mlpm_modules/cts-extensions/cts-extensions.xqy";

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
 : @param $query as `map:map` or `json:object`
 :)
declare %private function grpj:query-parser(
  $query,
  $fn-name as xs:QName
) as function(*)*
{
  (: rows, columns, or computes :)
  let $name := fn:local-name-from-QName($fn-name) || "s"
  for $entry in grpj:get-values($query, $name)
  let $ref := ctx:reference-from-map( (map:get($entry, "ref"), $entry)[1] )
  let $alias := map:get($entry, "alias")
  let $aggregate-fn := map:get($entry, "fn")
  let $is-compute := $fn-name eq xs:QName("cts:compute")
  let $arity :=
    if (fn:exists($alias) and $is-compute)
    then 3
    else
      if (fn:exists($alias) != $is-compute) (: xor :)
      then 2
      else 1
  let $fn := fn:function-lookup($fn-name, $arity)
  return
    switch($arity)
    case 3 return $fn($alias, $aggregate-fn, $ref)
    case 2 return $fn( if ($is-compute) then $aggregate-fn else $alias, $ref )
    default return $fn($ref)
};

(:~
 : evaluate map / JSON serialized cts:group-by query definitions
 :
 : @param $query as `map:map` or `json:object`
 :)
declare function grpj:query($query)
{
  let $rows := grpj:query-parser($query, xs:QName("cts:row"))
  let $columns := grpj:query-parser($query, xs:QName("cts:column"))
  let $computes := grpj:query-parser($query, xs:QName("cts:compute"))
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
