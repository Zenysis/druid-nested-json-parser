## Druid Nested JSON Parser Extension
The nested JSON parser extension is used during [Druid](http://druid.io/)
indexing to allow rows with nested JSON data to be unpacked and flattened into
a single row to be ingested.

### Installation
You can either use the prebuilt jar located in `dist/` or build from source
using the directions below.

Once you have the compiled `jar`, copy it to your druid installation and
follow the
[including extension](http://druid.io/docs/latest/operations/including-extensions.html)
druid documentation. You should end up adding a line similar to this to your
`common.runtime.properties` file:

`druid.extensions.loadList=["druid-nested-json-parser"]`

##### Building from source
Clone the druid repo and add this line to `pom.xml` in the "Community extensions"
section:

```xml
<module>${druid-nested-json-parser-src-root}/nested-json-parser</module>
```
replacing `${druid-nested-json-parser-src-root}` with your path to this
repo.

Then, inside the druid repo, run:
`mvn package -DskipTests=true -rf :druid-nested-json-parser`
This will build the nested JSON parser extension and place it in
`${druid-nested-json-parser-src-root}/nested-json-parser/target`


### Use
The nested JSON parser uses a `pivotSpec` to determine how to unpack nested JSON
values into a flat row.

XXXXXXXXXXX

```javascript
{
  ...
  "spec": {
    "dataSchema": {
      "dataSource": "MyDatasource",
      "parser": {
        "parseSpec": {
          ...
          "format": "json"
        },
        "type": "nestedJson",
        "pivotSpec": [{
          "dimensionFieldName": "field",
          "rowFieldName": "data",
          "metricFieldName": "val"
        }]
      },
      ...
    },
    ...
  },
  ...
}
```


### Example
XXXXXXXXXXXXX
