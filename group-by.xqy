xquery version "1.0-ml";

(:~
 : provides declarative syntax for grouping lexicon values and performing constrained aggregate computations
 :
 : depends on https://github.com/joemfb/cts-extensions
 :
 : @author Gary Vidal
 : @author Joe Bryan
 : @version 0.9
 :)
module namespace ext = "http://marklogic.com/cts";

import module namespace ctx = "http://marklogic.com/cts-extensions"
  at "/mlpm_modules/cts-extensions/cts-extensions.xqy";

declare option xdmp:mapping "false";

declare variable $cts:AGGREGATES :=
  map:new((
    map:entry("sum", cts:sum-aggregate(?, ?, ?)),
    map:entry("avg", cts:avg-aggregate(?, ?, ?)),
    map:entry("min", cts:min(?, ?, ?)),
    map:entry("max", cts:max(?, ?, ?)),
    map:entry("stddev", cts:stddev(?, ?, ?)),
    map:entry("stddev-population", cts:stddev-p(?, ?, ?)),
    map:entry("variance", cts:variance(?, ?, ?)),
    map:entry("variance-population", cts:variance-p(?, ?, ?)),
    map:entry("count", function($refs, $options, $query) {
      cts:assert-count($refs, 1, "cts:count-aggregate requires one reference"),
      cts:count-aggregate($refs, ("fragment-frequency", $options), $query)
    }),
    map:entry("median", function($refs, $options, $query) {
      cts:assert-count($refs, 1, "median requires one reference"),
      cts:median( cts:values($refs, (), $options, $query) )
    }),
    map:entry("covariance", function($refs, $options, $query) {
      cts:assert-count($refs, 2, "cts:covariance requires two references"),
      cts:covariance($refs[1], $refs[2], $options, $query)
    }),
    map:entry("covariance-population", function($refs, $options, $query) {
      cts:assert-count($refs, 2, "cts:covariance-p requires two references"),
      cts:covariance-p($refs[1], $refs[2], $options, $query)
    }),
    map:entry("correlation", function($refs, $options, $query) {
      cts:assert-count($refs, 2, "cts:correlation requires two references"),
      cts:correlation($refs[1], $refs[2], $options, $query)
    })));

declare %private function cts:assert-count($items, $count, $msg)
{
  if (fn:count($items) eq $count) then ()
  else fn:error((), "INCORRECT-COUNT", $msg)
};

(:~
 : Create a sequence of range queries, one for each cts:reference and value
 :
 : @param $ref as `cts:reference*` or `element(cts:*-reference)*`
 :)
declare %private function cts:reference-queries($refs, $values as xs:anyAtomicType*) as cts:query+
{
  let $size :=
    if ($values instance of json:array)
    then json:array-size($values)
    else fn:count($values)
  for $i in 1 to $size
  return ctx:reference-query($refs[$i], $values[$i])
};

(:~
 : Get 1-or-more `cts:reference` objects from `$ref-parent`
 :
 : @param $ref-parent as `element(cts:column)`, `element(cts:compute), or `element(cts:row)`
 :)
declare %private function cts:get-reference($ref-parent) as cts:reference*
{
  $ref-parent/cts:*[fn:matches(fn:local-name(.), "reference$")] ! cts:reference-parse(.)
};

declare %private function cts:member(
  $type as xs:QName,
  $alias as xs:string,
  $reference as cts:reference*,
  $options as xs:string*
) {
  cts:member($type, $alias, $reference, $options, ())
};

declare %private function cts:member(
  $type as xs:QName,
  $alias as xs:string,
  $reference as cts:reference*,
  $options as xs:string*,
  $custom as element()*
) {
  function () {
    element { $type } {
      element cts:alias { $alias },
      $custom,
      $reference,
      $options ! element cts:option { . }
    }
  }
};

(:~
 : Create a column reference
 :)
declare function cts:column($alias as xs:string, $reference as cts:reference)
{
  cts:column($alias, $reference, ())
};

declare function cts:column(
   $alias as xs:string,
   $reference as cts:reference,
   $options as xs:string*
) as (function() as element(cts:column))
{
  cts:member(xs:QName("cts:column"), $alias, $reference, $options)
};

declare function cts:row($alias as xs:string, $reference as cts:reference)
{
  cts:row($alias, $reference, ())
};

declare function cts:row(
  $alias as xs:string,
  $reference as cts:reference,
  $options as xs:string*
) as (function() as element(cts:row))
{
  cts:member(xs:QName("cts:row"), $alias, $reference, $options)
};

declare function cts:compute($function as xs:string, $reference as cts:reference*)
{
   cts:compute($function, $function, $reference, ())
};

declare function cts:compute(
  $alias as xs:string,
  $function as xs:string,
  $reference as cts:reference*
) {
   cts:compute($alias, $function, $reference, ())
};

declare function cts:compute(
  $alias as xs:string,
  $function as xs:string,
  $reference as cts:reference*,
  $options as xs:string*
) as (function() as element(cts:compute))
{
  cts:member(xs:QName("cts:compute"), $alias, $reference, $options, element cts:function { $function })
};


declare function cts:group-by($f as function(*)*)
{ cts:group-by($f, (), ()) };
declare function cts:group-by($f as function(*)*, $options as xs:string*)
{ cts:group-by($f, $options, ()) };

declare function cts:group-by(
  $f as function(*)*,
  $options as xs:string*,
  $query as cts:query?
) {
  cts:olap-complete( cts:olap-def(xs:QName("cts:group-by"), $f, $options, $query)() )
};

declare function cts:cross-product($f as function(*)*)
{ cts:cross-product($f, (), ()) };
declare function cts:cross-product($f as function(*)*, $options as xs:string*)
{ cts:cross-product($f, $options, ()) };

declare function cts:cross-product(
  $f as function(*)*,
  $options as xs:string*,
  $query as cts:query?
) {
  cts:olap-complete( cts:olap-def(xs:QName("cts:cross-product"), $f, $options, $query)() )
};

declare function cts:cube($f as function(*)*)
{ cts:cube($f, (), ()) };
declare function cts:cube($f as function(*)*, $options as xs:string*)
{ cts:cube($f, $options, ()) };

declare function cts:cube(
  $f as function(*)*,
  $options as xs:string*,
  $query as cts:query?
) {
  cts:olap-complete( cts:olap-def(xs:QName("cts:cube"), $f, $options, $query)() )
};

declare %private function cts:to-nested-array($seq) as json:array
{
  json:array(
    document {
      json:to-array(
        document { $seq }/* ) }/*)
};

declare %private function cts:olap-complete($def as element(cts:olap))
{
  let $wrap :=
    if (xs:boolean($def/cts:options/cts:headers))
    then function($x) {
      map:new((
        map:entry("headers", cts:olap-headers($def)),
        map:entry("results", $x)))
    }
    else function($x) { $x }
  return
    $wrap(
      if ($def/cts:options/cts:format eq "array")
      then cts:to-nested-array(cts:olap($def))
      else cts:olap($def))
};

declare %private function cts:olap-headers($def as element(cts:olap))
{
  let $arr := json:array()
  let $type := $def/(cts:group-by|cts:cross-product|cts:cube)
  return (
    for $x in $type/*
    return
      json:array-push($arr,
      if ($x/(self::cts:row|self::cts:column|self::cts:compute))
      then $x/cts:alias/fn:string()
      else
        if ($x/self::cts:olap)
        then cts:olap-headers($x)
        else ()),
    $arr
  )
};

declare %private function cts:olap-parse-options($options as xs:string*) as element(cts:options)
{
  let $f := function($key as xs:string, $options as xs:string*, $default as xs:string) {
    (fn:tokenize($options[fn:starts-with(., $key)], "=")[2], $default)[1]
  }
  return
    element cts:options {
      element cts:format { $f("format", $options, "array") },
      element cts:headers { $f("headers", $options, "false") },
      for $opt in $options[fn:not(fn:matches(., "^(headers|format)"))]
      return element cts:option { $opt }
    }
};

declare function cts:olap-def(
  $type as xs:QName,
  $f as function(*)*,
  $options as xs:string*,
  $query as cts:query?
) as function(*)
{
  function() {
    element cts:olap {
      element { $type } {
        let $defs := document { $f ! .() }
        return (
          $defs/cts:row,
          $defs/cts:column,
          $defs/cts:compute,
          $defs/cts:olap
        )
      },
      cts:olap-parse-options($options),
      element cts:query { $query }
    }
  }
};

(: returns an instance of the `$format` type :)
declare %private function cts:olap-output($format as xs:string) as item()
{
  switch($format)
    case "map" return map:map()
    case "array" return json:array()
    default return fn:error(xs:QName("UNKNOWN-FORMAT"), $format)
};

(: returns a consistent interface to `map:put` or `json:array-push()` :)
declare %private function cts:olap-format($output) as function(*)
{
  typeswitch($output)
    case map:map return map:put($output, ?, ?)
    case json:array return function($k, $v) { json:array-push($output, $v) }
    default return fn:error(xs:QName("UNKNOWN-OUTPUT-TYPE"), xdmp:describe($output))
};

declare function cts:olap($olap as element(cts:olap))
{ cts:olap($olap, (), ()) };
declare function cts:olap($olap as element(cts:olap), $options as element(cts:options))
{ cts:olap($olap, $options, ()) };

declare function cts:olap($olap as element(cts:olap), $options as element(cts:options)?, $q as cts:query?)
{
  let $query := cts:and-query(($q, $olap/cts:query/* ! cts:query(.)))
  (: TODO: union :)
  let $options := ($options, $olap/cts:options)[1]

  (: TODO: cts:to-nested-array(), alternate format-fn ? :)
  let $format := $olap/cts:options/cts:format/fn:string()

  for $def in $olap/(cts:group-by|cts:cross-product|cts:cube)
  let $members :=
    typeswitch($def)
      case element(cts:group-by) return $def/(cts:row,cts:column)
      case element(cts:cube) return $def/cts:row[1]
      default return $def/cts:row
  return cts:olap-impl(fn:node-name($def), $members, $def/cts:compute, $options, $query)
};

declare %private function cts:olap-impl(
  $type as xs:QName,
  $members as element()+,
  $computes as element()*,
  $options as element(cts:options),
  $query as cts:query?
) {
  let $format := $options/cts:format/fn:string()
  (: TODO: $options/cts:option/fn:string() ?? :)
  let $value-options := ($options/* except $options/(cts:format|cts:headers))/fn:string()
  let $refs := $members/cts:get-reference(.)

  for $tuple in cts:value-tuples($refs, $value-options, $query)
  let $compute-query := cts:and-query(($query, cts:reference-queries($refs, $tuple)))
  let $output := cts:olap-output($format)

  let $format-fn := cts:olap-format($output)
  let $compute-fn :=
    function() {
      for $comp in $computes
      return
        if ($comp/cts:function eq "frequency")
        then $format-fn($comp/cts:alias/fn:string(), cts:frequency($tuple))
        else $format-fn($comp/cts:alias/fn:string(), cts:compute-aggregate($comp, $compute-query))
    }
  let $columns-fn :=
    function() {
      $format-fn(
        "columns", (: TODO: alias? :)
        cts:olap-impl(xs:QName("cts:group-by"), $members/../cts:column, $computes, $options, $compute-query))
    }

  return (
    for $i in 1 to json:array-size($tuple)
    return $format-fn($members[$i]/cts:alias/fn:string(), $tuple[$i])
    ,
    switch($type)
      case xs:QName("cts:group-by") return (
        $compute-fn(),
        (: only group-by is recursive :)
        $members/../cts:olap ! $format-fn("olap", cts:olap(., $options, $compute-query))
      )
      case xs:QName("cts:cross-product") return
        $columns-fn()
      case xs:QName("cts:cube") return (
        $compute-fn(),
        $columns-fn(),
        if (fn:exists($members/following-sibling::cts:row))
        then
          $format-fn(
            "row-next", (: TODO: alias? :)
            cts:olap-impl(xs:QName("cts:cube"), $members/following-sibling::cts:row[1], $computes, $options, $compute-query))
        else ()
      )
      default return fn:error(xs:QName("UNKNOWN-OLAP-DEF"), $members/ancestor::*[fn:last()])
    ,
    $output
  )
};

declare function cts:compute-aggregate($comp)
{
  cts:compute-aggregate($comp, cts:and-query(()))
};
declare function cts:compute-aggregate($comp as element(cts:compute), $compute-query as cts:query)
{
  let $aggregate :=
    if (fn:matches($comp/cts:function, "^native/(.*)/(.*)$"))
    then cts:native-aggregate($comp/cts:function)
    else map:get($cts:AGGREGATES, $comp/cts:function)
  return
    if (fn:not(fn:exists($aggregate)))
    then fn:error(xs:QName("UNKNOWN-COMPUTATION"), $comp)
    else $aggregate( $comp/cts:get-reference(.), $comp/cts:options/cts:option/fn:string(), $compute-query )
};

declare %private function cts:native-aggregate($function as xs:string) as function(*)
{
  let $groups := fn:analyze-string($function, "^native/(.*)/(.*)$")//fn:group
  let $plugin := "native/" || $groups[1]/fn:string()
  let $name := $groups[2]/fn:string()
  return
    function($refs, $options, $query) {
      cts:aggregate($plugin, $name, $refs, (), $options, $query)
    }
};
