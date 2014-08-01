xquery version "1.0-ml";

(:~
 : methods for discovering indices (grouped by root QNames) and processing serialized cts:group-by query definitions
 :
 : @author Joe Bryan
 : @version 0.1
 :)
module namespace config = "http://marklogic.com/cts/group-by-config";

import module namespace ext = "http://marklogic.com/cts" at "/lib/group-by.xqy";
import module namespace ctx = "http://marklogic.com/cts-extensions" at "/lib/cts-extensions.xqy";
import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";

declare function config:element-indices($map, $config, $database)
{
  for $index in admin:database-get-range-element-indexes($config, $database)
  for $ref in element tmp { ctx:resolve-reference-from-index($index) }/*
  let $el-qname := fn:QName($ref/cts:namespace-uri, $ref/cts:localname)
  for $root in ctx:root-QNames($el-qname)
  let $key := xdmp:key-from-QName($root)
  return map:put($map, $key, ($ref, map:get($map, $key)))
};

declare function config:attribute-indices($map, $config, $database)
{
  for $index in admin:database-get-range-element-attribute-indexes($config, $database)
  for $ref in element tmp { ctx:resolve-reference-from-index($index) }/*
  let $query :=
    let $el-qname := fn:QName($ref/cts:parent-namespace-uri, $ref/cts:parent-localname)
    let $attr-qname := fn:QName($ref/cts:namespace-uri, $ref/cts:localname)
    return ctx:element-attribute-query($el-qname, $attr-qname)
  for $root in ctx:root-QNames($query)
  let $key := xdmp:key-from-QName($root)
  return map:put($map, $key, ($ref, map:get($map, $key)))
};

declare function config:path-indices($map, $config, $database)
{
  for $index in admin:database-get-range-path-indexes($config, $database)
  for $ref in element tmp { ctx:resolve-reference-from-index($index) }/*
  let $path := $ref/cts:path-expression/fn:string()
  let $query := ctx:path-query($path)
  for $root in ctx:root-QNames($query)
  let $key := xdmp:key-from-QName($root)
  return map:put($map, $key, ($ref, map:get($map, $key)))
};

declare function config:all-indices() as map:map
{
  let $map := map:map()
  let $config := admin:get-configuration()
  let $database := xdmp:database()
  return (
    config:element-indices($map, $config, $database),
    config:attribute-indices($map, $config, $database),
    config:path-indices($map, $config, $database),
    (: TODO: implement field, geospatial, etc. :)
    $map
  )
};

declare function config:aggregates() as xs:string+
{
  for $agg in map:keys($cts:AGGREGATES)
  order by $agg
  return $agg
};

declare function config:reference-to-map($ref) as map:map
{
  let $ref :=
    if ($ref instance of element())
    then $ref
    else element tmp { $ref }/*
  return
    map:new((
      map:entry("ref-type", fn:local-name($ref)),
      for $x in $ref/*
      return map:entry(fn:local-name($x), $x/fn:string())))
};

declare function config:reference-from-map($map)
{
  let $ref-type := map:get($map, "ref-type")
  return
    if (fn:exists($ref-type))
    then
      element { "cts:" || $ref-type } {
        for $key in map:keys($map)[. ne "ref-type"]
        return
          element { "cts:" || $key } {
            map:get($map, $key)
          }
      } ! cts:reference-parse(.)
    else ()
};

declare function config:docs() as map:map
{
  let $indices := config:all-indices()
  return
    map:new(
      for $key in map:keys($indices)
      return
        map:entry($key,
          for $val in map:get($indices, $key)
          return config:reference-to-map($val)))
};

declare function config:config() as map:map
{
  map:new((
    map:entry("aggregates", config:aggregates()),
    map:entry("docs", config:docs())))
};

declare function config:values($map, $key)
{
  let $obj := map:get($map, $key)
  return
    if ($obj instance of map:map+)
    then $obj
    else
      if ($obj instance of json:array)
      then json:array-values($obj)
      else ()
};

declare function config:query($query as map:map)
{
  (:
    TODO:
    let $result-type := map:get($query, "result-type")
    assert($result-type = ("group-by", "cross-product", "cube"))

    (hardcoded until more complex UI types developed)
  :)
  let $result-type := "group-by"

  (: TODO: refactor into utility methods :)
  let $rows :=
    for $row in config:values($query, "rows")
    let $ref := (map:get($row, "ref"), $row)[1]
    let $alias := (map:get($row, "alias"), map:get($ref, "localname"), map:get($ref, "path-expression"))[1]
    return cts:row($alias, config:reference-from-map($ref))
  let $columns :=
    for $col in config:values($query, "columns")
    let $ref := (map:get($col, "ref"), $col)[1]
    let $alias := (map:get($col, "alias"), map:get($ref, "localname"), map:get($ref, "path-expression"))[1]
    return cts:column($alias, config:reference-from-map($ref))
  let $computes :=
    for $comp in config:values($query, "computes")
    let $ref := (map:get($comp, "ref"), $comp)[1]
    let $fn := map:get($comp, "fn")
    let $alias := (map:get($comp, "alias"), (map:get($ref, "localname"), map:get($ref, "path-expression"))[1] || "-" || $fn)[1]
    return cts:compute($alias, $fn, config:reference-from-map($ref))
  let $options := config:values($query, "options")
  let $fn :=
    switch($result-type)
    case "group-by" return cts:group-by(?, ?)
    case "cross-product" return cts:cross-product(?, ?)
    case "cube" return cts:cube(?, ?)
    default return fn:error(xs:QName("UNKOWN-FN"), $result-type)
  return $fn( ($rows, $columns, $computes), $options )
};
