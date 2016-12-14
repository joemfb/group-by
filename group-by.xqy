xquery version "1.0-ml";

(:~
 : provides declarative syntax for grouping lexicon values and performing constrained aggregate computations
 :
 : depends on https://github.com/joemfb/cts-extensions
 :
 : @author Gary Vidal
 : @author Joe Bryan
 :)
module namespace ext = "http://marklogic.com/cts";

import module namespace ctx = "http://marklogic.com/cts-extensions"
  at "/ext/mlpm_modules/cts-extensions/cts-extensions.xqy";

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
      cts:count-aggregate(
        $refs,
        ($options,
        if ($options = ("item-frequency", "fragment-frequency")) then ()
        else "item-frequency"),
        $query)
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

declare variable $cts:olap-default-custom-options := map:new((
  map:entry("format", "array"),
  map:entry("headers", "false")
));

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
declare %private function cts:reference-queries($refs, $values as xs:anyAtomicType*) as cts:query*
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
declare %private function cts:get-reference($ref-parent as element()) as cts:reference+
{
  $ref-parent/(
    cts:uri-reference|
    cts:collection-reference|
    cts:element-reference|
    cts:json-property-reference|
    cts:element-attribute-reference|
    cts:path-reference|
    cts:field-reference|
    cts:geospatial-attribute-pair-reference|
    cts:geospatial-element-pair-reference|
    cts:geospatial-json-property-pair-reference|
    cts:geospatial-element-child-reference|
    cts:geospatial-json-property-child-reference|
    cts:geospatial-element-reference|
    cts:geospatial-json-property-reference|
    cts:geospatial-path-reference
  )/cts:reference-parse(.)
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
declare function cts:column($reference as cts:reference)
{
  cts:column(ctx:reference-alias($reference), $reference, ())
};

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

declare function cts:row($reference as cts:reference)
{
  cts:row(ctx:reference-alias($reference), $reference, ())
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
  let $alias := $function ||
    (if (fn:exists($reference))
    then "-" || ctx:reference-alias($reference)
    else ())
  return cts:compute($alias, $function, $reference)
};

declare function cts:compute(
  $alias as xs:string,
  $function as xs:string,
  $reference as cts:reference*
) {
  cts:compute($alias, $function, $reference, "concurrent")
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

(: FIXME: not accurate for cross-product and cube strategies, where recursion is implicit instead of explicit :)
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

declare %private function cts:olap-custom-options($options as xs:string*) as element()+
{
  for $key in map:keys($cts:olap-default-custom-options)
  return
    element { "cts:" || $key } {
      fn:head((
        fn:tokenize($options[fn:starts-with(., $key)], "=")[2],
        map:get($cts:olap-default-custom-options, $key) ))
    }
};

declare %private function cts:olap-option-type($option as xs:string) as xs:string?
{
  switch($option)
  case "frequency-order" return "order"
  case "item-order" return "order"
  case "fragment-frequency" return "frequency"
  case "item-frequency" return "frequency"
  case "ascending" return "direction"
  case "descending" return "direction"
  case "eager" return "lazy"
  case "lazy" return "lazy"
  case "score-logtfidf" return "score"
  case "score-logtf" return "score"
  case "score-simple" return "score"
  case "score-random" return "score"
  case "score-zero"  return "score"
  case "any" return "scope"
  case "document" return "scope"
  case "properties" return "scope"
  case "locks" return "scope"
  default return ()
};

declare function cts:olap-parse-options($options as xs:string*) as element()+
{
  element cts:options {
    cts:olap-custom-options($options),
    for $option in $options
    let $tokens := fn:tokenize($option, "=")
    where
      (: exclude custom options :)
      if (fn:contains($option, "="))
      then fn:not(map:contains($cts:olap-default-custom-options, $tokens[1]))
      else fn:true()
    return
      element cts:option {
        if (fn:contains($option, "="))
        then (
          attribute key { $tokens[1] },
          attribute value { $tokens[2] }
        )
        else attribute type { cts:olap-option-type($option) }[. ne ""]
        ,
        $option
      }
  }
};

declare function cts:olap-merge-options(
  $a as element(cts:options)?,
  $b as element(cts:options)?
) as element(cts:options)
{
  if (fn:not(fn:exists($a)) or fn:not(fn:exists($b)))
  then fn:exactly-one(($a, $b))
  else
    let $except-a := json:array()
    return
      element cts:options {
        $a/(cts:format|cts:headers),

        for $option in $b/cts:option[@type|@key]
        let $original :=
          fn:zero-or-one(
            $a/cts:option[@type eq $option/@type or @key eq $option/@key])
        return
          if (fn:exists($original))
          then ($original, json:array-push($except-a, $original))
          else $option,

        for $val in fn:distinct-values((
          $a/cts:option except json:array-values($except-a),
          $b/cts:option[fn:not(@type)][fn:not(@key)]
        ))
        return
          element cts:option {
            $a/cts:option[. eq $val]/@*,
            $val
          }
      }
};

(: TODO: validate definition :)
(: TODO: expand recursion strategies here, rather than during evalution :)
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

declare %private function cts:olap-initial-members($def as element())
{
  typeswitch($def)
  case element(cts:cross-product) return $def/cts:row
  case element(cts:cube) return $def/cts:row[1]
  default return $def/(cts:row,cts:column)
};

declare function cts:olap($olap as element(cts:olap))
{ cts:olap($olap, (), ()) };
declare function cts:olap($olap as element(cts:olap), $options as element(cts:options))
{ cts:olap($olap, $options, ()) };

declare function cts:olap($olap as element(cts:olap), $options as element(cts:options)?, $q as cts:query?)
{
  let $def := fn:exactly-one($olap/(cts:group-by|cts:cross-product|cts:cube))
  return
    cts:olap-impl(
      fn:node-name($def),
      cts:olap-initial-members($def),
      $def/cts:compute,
      cts:olap-merge-options($options, $olap/cts:options),
      cts:and-query(($q, $olap/cts:query/cts:*/cts:query(.))))
};

declare %private function cts:olap-impl(
  $type as xs:QName,
  $members as element()*,
  $computes as element()*,
  $options as element(cts:options),
  $query as cts:query?
) {
  if (fn:exists($members))
  then
    let $refs := $members/cts:get-reference(.)
    return
      for $tuple in cts:value-tuples($refs, $options/cts:option, $query)
      return cts:olap-produce-output($type, $members, $computes, $options, $query, $refs, $tuple)
  else cts:olap-produce-output($type, $members, $computes, $options, $query, (), ())
};

declare %private function cts:olap-produce-output(
  $type as xs:QName,
  $members as element()*,
  $computes as element()*,
  $options as element(cts:options),
  $query as cts:query?,
  $refs as cts:reference*,
  $tuple as json:array*
) {
  let $compute-query := cts:and-query(($query, cts:reference-queries($refs, $tuple)))
  let $output := cts:olap-output($options/cts:format)

  let $format-fn := cts:olap-format($output)
  let $compute-fn := function() {
    $computes ! $format-fn(./cts:alias,
      if (./cts:function eq "frequency" and fn:exists($tuple))
      then cts:frequency($tuple)
      else cts:compute-aggregate(., $compute-query)
    )
  }
  let $columns-fn := function() {
    $format-fn("columns", (: TODO: alias? :)
      cts:olap-impl(xs:QName("cts:group-by"), $members/../cts:column, $computes, $options, $compute-query))
  }

  return (
    (: append $tuple values to $output :)
    for $i in 1 to json:array-size($tuple)
    return $format-fn($members[$i]/cts:alias, $tuple[$i]),

    (: compute and append aggregate computations, apply type-specific recursion strategies :)
    switch($type)
    case xs:QName("cts:group-by") return (
      $compute-fn(),
      $members/parent::cts:group-by/cts:olap ! $format-fn("olap", (: TODO: alias? :)
        cts:olap(., $options, $compute-query)
      )
    )
    case xs:QName("cts:cross-product") return
      $columns-fn()
    case xs:QName("cts:cube") return (
      $compute-fn(),
      $columns-fn(),
      let $next := $members/following-sibling::cts:row[1]
      return
        if (fn:not(fn:exists($next))) then ()
        else $format-fn($next/cts:alias || "-cube",
          cts:olap-impl(xs:QName("cts:cube"), $next, $computes, $options, $compute-query)
        )
    )
    default return fn:error(xs:QName("UNKNOWN-OLAP-DEF"), xdmp:describe($members/ancestor::*[fn:last()])),

    (: return output :)
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
    if (fn:matches($comp/cts:function, "^native/.*/.*$"))
    then cts:native-aggregate($comp/cts:function)
    else map:get($cts:AGGREGATES, $comp/cts:function)
  return
    if (fn:not(fn:exists($aggregate)))
    then fn:error((), "UNKNOWN-COMPUTATION", xdmp:describe($comp))
    else
      $aggregate(
        $comp/cts:get-reference(.),
        $comp/cts:options/cts:option,
        $compute-query)
};

declare %private function cts:native-aggregate($function as xs:string) as function(*)
{
  let $groups := fn:analyze-string($function, "^(native/.*)/(.*)$")//fn:group
  return function($refs, $options, $query) {
    cts:aggregate($groups[1], $groups[2], $refs, (), $options, $query)
  }
};
