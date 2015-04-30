xquery version "1.0-ml";

(:~
 : MarkLogic REST API extension for JSON `cts:group-by` queries over any database
 :
 : @author Joe Bryan
 :)
module namespace ext = "http://marklogic.com/rest-api/resource/group-by";

import module namespace grpj = "http://marklogic.com/cts/group-by/json"
  at "/ext/mlpm_modules/group-by/group-by-json.xqy";

declare namespace roxy = "http://marklogic.com/roxy";

declare option xdmp:mapping "false";

declare %private function ext:capture-metrics($results, $t-minus-0)
{
  let $metrics := map:entry("total-time", xdmp:elapsed-time() - $t-minus-0)
  return
    if ($results instance of map:map)
    then (
      map:put($results, "metrics", $metrics),
      $results
    )
    else
      map:new((
        map:entry("results", results),
        map:entry("metrics", $metrics)))
};

declare %private function ext:invoke-in($params, $fn)
{
  let $database :=
    if (map:contains($params, "database"))
    then xdmp:database(map:get($params, "database"))
    else xdmp:database()
  return
    xdmp:invoke-function($fn,
      <options xmlns="xdmp:eval">
        <database>{ $database }</database>
      </options>)
};

(:~
 : evaluates a JSON-serialized `cts:group-by` query
 :)
declare
  %roxy:params("database=xs:string?")
function ext:post(
  $context as map:map,
  $params as map:map,
  $input as document-node()*
) as document-node()*
{
  map:put($context, "output-types", "application/json"),
  (: TODO: catch errors, set status codes :)

  let $t-minus-0 := xdmp:elapsed-time()
  let $query := xdmp:from-json($input)
  return
    document {
      ext:invoke-in($params, function() {
        xdmp:to-json(
          ext:capture-metrics(
            grpj:query($query),
            $t-minus-0))
      })
    }
};
