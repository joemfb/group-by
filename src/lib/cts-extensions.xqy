xquery version "1.0-ml";

(:~
 : experimental extension query types
 :
 : @author Joe Bryan
 : @version 0.1
 :)
module namespace ctx = "http://marklogic.com/cts-extensions";

declare namespace db = "http://marklogic.com/xdmp/database";
declare namespace qry = "http://marklogic.com/cts/query";
declare namespace idxpath = "http://marklogic.com/indexpath";

declare private function ctx:declare-ns($qnames as xs:QName*) as xs:string*
{
  for $qname at $i in $qnames ! fn:namespace-uri-from-QName(.)
  where $qname ne ""
  return 'declare namespace _' || fn:string($i) || '= "' || $qname || '";'
};

declare private function ctx:create-QName-path($qnames as xs:QName*) as xs:string*
{
  for $qname at $i in $qnames
  let $prefix := ("_" || fn:string($i) || ":")[fn:namespace-uri-from-QName($qname) ne ""]
  return $prefix || fn:local-name-from-QName($qname)
};

declare private function ctx:root-element-query-term($qname as xs:QName) as xs:unsignedLong
{
  let $prolog := ctx:declare-ns($qname)
  let $query := ctx:create-QName-path($qname)
  let $plan := xdmp:eval($prolog || "xdmp:plan(/" || $query || ")")/qry:final-plan
  return fn:exactly-one($plan//qry:term-query[fn:starts-with(qry:annotation, "doc-root(")]/qry:key)/fn:data(.)
};

declare private function ctx:element-child-query-term($qname as xs:QName, $child as xs:QName) as xs:unsignedLong
{
  let $prolog := fn:string-join(ctx:declare-ns(($qname, $child)), "")
  let $query := fn:string-join(ctx:create-QName-path(($qname, $child)), "/")
  let $plan := xdmp:eval($prolog || "xdmp:plan(/" || $query || ")")/qry:final-plan
  return fn:exactly-one($plan//qry:term-query[fn:starts-with(qry:annotation, "element-child(")]/qry:key)/fn:data(.)
};

declare function ctx:root-element-query($qname as xs:QName) as cts:query
{ ctx:root-element-query($qname, ()) };

declare function ctx:root-element-query($qname as xs:QName, $query as cts:query?) as cts:query
{
  cts:and-query((
    cts:term-query(
      ctx:root-element-query-term($qname), 0),
    $query))
};

declare function ctx:element-child-query($qname as xs:QName, $child as xs:QName)
{ ctx:element-child-query($qname, $child, ()) };

declare function ctx:element-child-query($qname as xs:QName, $child as xs:QName, $query as cts:query?)
{
  cts:and-query((
    cts:term-query(
      ctx:element-child-query-term($qname, $child), 0),
    $query))
};

declare function ctx:root-QNames() as xs:QName*
{ ctx:root-QNames((), ()) };

declare function ctx:root-QNames($arg) as xs:QName*
{
  let $query :=
    if ($arg instance of cts:query)
    then $arg
    else
      if ($arg instance of xs:QName)
      then cts:element-query($arg, cts:and-query(()))
      else fn:error(xs:QName("UNKNOWN-TYPE"), (xdmp:describe($arg), $arg))
  return ctx:root-QNames($query, ())
};

declare function ctx:root-QNames($query as cts:query?, $excluded-roots as xs:QName*) as xs:QName*
{
  let $not-query := cts:not-query($excluded-roots ! ctx:root-element-query(.))
  let $root := fn:node-name( cts:search(/element(), cts:and-query(( $query, $not-query )), "unfiltered")[1] )
  return $root ! (., ctx:root-QNames($query, ($excluded-roots, .)))
};

declare private function ctx:element-attribute-query-term($qname as xs:QName, $attr as xs:QName) as xs:unsignedLong
{
  let $prolog := fn:string-join(ctx:declare-ns(($qname, $attr)), "")
  let $query := fn:string-join(ctx:create-QName-path(($qname, $attr)), "/@")
  let $plan := xdmp:eval($prolog || "xdmp:plan(/" || $query || ")")/qry:final-plan
  return fn:exactly-one($plan//qry:term-query[fn:starts-with(qry:annotation, "element-attribute(")]/qry:key)/fn:data(.)
};

(: requires further testing; doesn't seem to work in some cases :)
declare function ctx:element-attribute-query($qname as xs:QName, $attr as xs:QName)
{ ctx:element-attribute-query($qname, $attr, ()) };

declare function ctx:element-attribute-query($qname as xs:QName, $attr as xs:QName, $query as cts:query?)
{
  cts:and-query((
    cts:term-query(
      ctx:element-attribute-query-term($qname, $attr), 0),
    $query))
};

declare function ctx:path-query($path-expression as xs:string)
{
  (: and-query too restrictive, this is too permissive, requires more research :)
  cts:or-query(
    cts:index-path-keys($path-expression)/idxpath:reindexer-keys/* ! cts:term-query(., 0))
};

declare function ctx:db-path-namespaces() as xs:string*
{ ctx:db-path-namespaces(xdmp:database()) };

declare function ctx:db-path-namespaces($database-id as xs:unsignedLong) as xs:string*
{
  for $ns in xdmp:database-path-namespaces($database-id)/db:path-namespace
  return (
    $ns/db:prefix/fn:string(),
    $ns/db:namespace-uri/fn:string()
  )
};

declare function ctx:resolve-reference-from-index($node) as cts:reference*
{
  let $options :=
  (
    $node/db:scalar-type ! fn:concat("type=", .),
    if (fn:not($node/db:scalar-type = ("date", "int", "decimal")))
    then
      $node/db:collation ! fn:concat("collation=", .)
    else ()
    (:
      TODO:
      $node/db:coordinate-system ! fn:concat("coordinate-system", .)
      unchecked?,
      other options?
    :)
  )
  return
    typeswitch($node)
      case element(db:range-element-index) return
        cts:element-reference(
          for $elem in fn:tokenize($node/db:localname/fn:string(), " ")
          return fn:QName($node/db:namespace-uri, $elem),
          $options
        )
      case element(db:range-element-attribute-index) return
        cts:element-attribute-reference(
          (: TODO: tokenize names? :)
          for $elem in $node/db:parent-localname
          return fn:QName($node/db:parent-namespace-uri, $elem),
          for $attr in $node/db:localname
          return fn:QName($node/db:namespace-uri, $attr),
          $options
        )
      case element(db:range-path-index) return
        xdmp:with-namespaces(
          ctx:db-path-namespaces(),
          cts:path-reference($node/db:path-expression, $options))
      (: TODO: process other reference types :)
      case element(cts:field-reference) return ()
      case element(cts:uri-reference) return ()
      case element(cts:collection-reference) return ()
      case element(cts:geospatial-attribute-pair-reference) return ()
      case element(cts:geospatial-element-pair-reference) return ()
      case element(cts:geospatial-element-child-reference) return ()
      case element(cts:geospatial-element-reference) return ()
      default return fn:error((),"Unknown Reference Type", $node)
};
