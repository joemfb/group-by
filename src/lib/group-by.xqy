xquery version "1.0-ml";

module namespace ext = "http://marklogic.com/cts";

declare default collation "http://marklogic.com/collation/";

declare option xdmp:mapping "false";

(:~
 : Resolves a reference from an xml representation to a cts:reference
~:)
declare function cts:resolve-reference($node) as cts:reference {
   typeswitch($node) 
     case element(cts:element-reference) return 
        cts:element-reference(
          for $elem in $node/cts:localname
          return fn:QName($node/cts:namespace-uri,$elem),
          ($node/cts:scalar-type ! fn:concat("type=", .),
           $node/cts:collation ! fn:concat("collation=", .),
           $node/cts:coordinate-system ! fn:concat("coordinate-system", .),
           $node/cts:nullable ! "nullable"
          )
       )
    case element(cts:element-attribute-reference) return
      cts:element-attribute-reference(
          for $elem in $node/cts:parent-localname
          return fn:QName($node/cts:parent-namespace-uri,$elem),
          for $attr in $node/cts:localname
          return fn:QName($node/cts:namespace-uri,$attr),
          ($node/cts:scalar-type       ! fn:concat("type=", .),
           $node/cts:collation         ! fn:concat("collation=", .),
           $node/cts:coordinate-system ! fn:concat("coordinate-system", .),
           $node/cts:nullable          ! "nullable"
          )
      )
    case element(cts:path-reference) return 
      cts:path-reference(
         $node/cts:path,
         (
           $node/cts:scalar-type ! fn:concat("type=", .),
           $node/cts:collation ! fn:concat("collation=", .),
           $node/cts:coordinate-system ! fn:concat("coordinate-system", .),
           $node/cts:nullable ! "nullable"
         )
      )
         
    case element(cts:field-reference) return 
         cts:field-reference(
           $node/cts:name,
          (
             $node/cts:collation ! fn:concat("collation=", .),
             $node/cts:coordinate-system ! fn:concat("coordinate-system", .),
             $node/cts:nullable ! "nullable"
           )
        )
    case element(cts:uri-reference) return  ()
    case element(cts:collection-reference) return ()
    case element(cts:geospatial-attribute-pair-reference) return ()
    case element(cts:geospatial-element-pair-reference) return ()
    case element(cts:geospatial-element-child-reference) return ()
    case element(cts:geospatial-element-reference) return ()
    default return fn:error((),"Unknown Reference Type",$node)
    
};
(:~
 : Resolves a cts:reference to a query given a set of values
~:)
declare function cts:reference-query($node,$values as xs:anyAtomicType*) as cts:query {
   let $node := if($node instance of element()) then $node else <x>{$node}</x>/*
   let $ref :=  $node
   return
   typeswitch($ref) 
     case element(cts:element-reference) return 
        cts:element-range-query(
          for $elem in $ref/cts:localname
          return fn:QName($ref/cts:namespace-uri,$elem),
          "=",
          $values,
          (
           $ref/cts:collation ! fn:concat("collation=", .),
           $ref/cts:coordinate-system ! fn:concat("coordinate-system", .)
          )
       )
    case element(cts:element-attribute-reference) return
      cts:element-attribute-range-query(
          for $elem in $ref/cts:parent-localname
          return fn:QName($ref/cts:parent-namespace-uri,$elem),
          for $attr in $ref/cts:localname
          return fn:QName($ref/cts:namespace-uri,$attr),
          "=",
          $values,
          ($ref/cts:scalar-type ! fn:concat("type=", .),
           $ref/cts:collation ! fn:concat("collation=", .),
           $ref/cts:coordinate-system ! fn:concat("coordinate-system", .)
          )
      )
    case element(cts:path-reference) return 
      cts:path-range-query(
         $ref/cts:path,
         "=",
         $values,
         (
           $ref/cts:scalar-type ! fn:concat("type=", .),
           $ref/cts:collation ! fn:concat("collation=", .),
           $ref/cts:coordinate-system ! fn:concat("coordinate-system", .)
         )
      )
         
    case element(cts:field-reference) return 
         cts:field-range-query(
                 $ref/cts:name,
                 "=",
                 $values,
                 (
                   $ref/cts:scalar-type ! fn:concat("type=", .),
                   $ref/cts:collation ! fn:concat("collation=", .),
                   $ref/cts:coordinate-system ! fn:concat("coordinate-system", .)
                 )
              )
    case element(cts:uri-reference) return 
       cts:document-query(
          $values
       )
    case element(cts:collection-reference) return 
       cts:collection-query(
         $values
       )
    case element(cts:geospatial-attribute-pair-reference) return ()
    case element(cts:geospatial-element-pair-reference) return ()
    case element(cts:geospatial-element-child-reference) return ()
    case element(cts:geospatial-element-reference) return ()
    default return fn:error(xs:QName("REFERENCE-QUERY-ERROR"),"Unknown Reference Type to create query",$ref)
};

declare function cts:get-reference($ref-parent) {
   $ref-parent/(cts:element-reference|cts:path-reference|cts:element-attribute-reference|cts:field-reference|cts:collection-reference|cts:uri-reference) ! cts:resolve-reference(.)
};

(:~
 : Converts literals and functions to sequence of (cts:column) definitions
 :)
declare function cts:columns(
 $cols as function() as element(cts:column)*
) as element(cts:column)*
{
   for $col in $cols
   return
     if($col instance of function(item()*) as element(cts:column)*) then $col()
     else if($col instance of element(cts:column)) then $col
     else fn:error(xs:QName("CAST-ERROR"),"Cannot Cast Type",xdmp:describe($col,(),()))
};

(:~
 : Create a column reference
 :)
declare function cts:column(
  $alias as xs:string,
  $reference as cts:reference
) {
  cts:column($alias,$reference,())
};

declare function cts:column(
   $alias as xs:string?,
   $reference as cts:reference?,
   $options as xs:string*
)  {
   function () {
     element cts:column {
       <cts:alias>{$alias}</cts:alias>,
       $reference,
       for $option in $options return <cts:option>{$option}</cts:option>
     }
  }
};

declare function cts:row(
  $alias as xs:string,
  $reference as cts:reference
) {
  cts:row($alias,$reference,())
};

declare function cts:row(
   $alias as xs:string?,
   $reference as cts:reference?,
   $options as xs:string*
) as function(*) {
  function() { 
   element cts:row {
       <cts:alias>{$alias}</cts:alias>,
       $reference,
       for $option in $options return <cts:option>{$option}</cts:option>
     }
  }
};

declare function cts:compute(
  $alias as xs:string,
  $function as xs:string,
  $reference as cts:reference*
) {
   cts:compute($alias,$function,$reference,())
};

declare function cts:compute(
  $alias as xs:string,
  $function as xs:string,
  $reference as cts:reference,
  $options as xs:string*
) {
  function () {
    <cts:compute>{
       <cts:alias>{$alias}</cts:alias>,
       <cts:function>{$function}</cts:function>,
       $reference,
       for $opt in $options return <cts:compute-option>{$opt}</cts:compute-option>
    }</cts:compute>
  }
};
declare function cts:results($item as item()*) as item()* {
 $item
};

declare function cts:groupby(
  $columns as (function() as element(cts:column)*)*,
  $compute as item()*)
{
  cts:groupby($columns,$compute,(),())
}; 
declare function cts:groupby(
  $columns as (function() as element(cts:column)*)*,
  $compute as item()*,
  $options as xs:string*
) {
  cts:groupby($columns,$compute,$options,())
};
(:~
 : Calculates a group by value-tuples and computes the aggregate for each tuple value array
~:)
declare function cts:groupby(
  $columns as (function() as element(cts:column)*)*,
  $compute as item()*,
  $options as xs:string*,
  $query as cts:query?
) {
   let $def := 
       <cts:groupby>
        {$columns ! .()}
        {$compute ! .()}
       </cts:groupby>
   let $col-refs     := $def/cts:column/cts:get-reference(.)
   let $compute-refs := $def/cts:compute
   let $tuples := cts:value-tuples(($col-refs),$options,$query)
   for $c at $cpos in $tuples
   let $aggs :=
         for $comp at $spos in $compute-refs
         let $compute-query := cts:and-query((
           $query,
           for $ci in json:array-size($c) return cts:reference-query($col-refs[$ci], $c[$ci])
         ))
         return 
           switch($comp/cts:function)
             case "sum" return cts:sum-aggregate($comp/cts:get-reference(.),(),$compute-query)
             case "avg" return cts:avg-aggregate($comp/cts:get-reference(.),(),$compute-query)
             case "min" return cts:min($comp/cts:get-reference(.),(),$compute-query)
             case "max" return cts:max($comp/cts:get-reference(.),(),$compute-query)
             case "count" return cts:count-aggregate($comp/cts:get-reference(.),(),(),$compute-query)
             default return cts:aggregate("",fn:string($comp/cts:function),$comp/cts:get-reference(.),(),(),$compute-query)
  let $_ :=  $aggs !  json:array-push($c,.)
  return $c
};