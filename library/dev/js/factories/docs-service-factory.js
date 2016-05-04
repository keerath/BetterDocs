(function( module ) {

	module
  .factory('docsService', [
    'http','model',
    function(
      http,model
    ) {

      var settings;

      function buildSearchString(str) {
        var result = '';
        if (str[0] === '\'') {
          result = str.substr(1, str.length - 2);
        } else {
          result = str.split(',').map(function(entry) {
            return '*' + entry.trim();
          }).join(',');
        }
        return result;
      }

      function search( obj ) {


        var correctedQuery
          , queryBlock,token,methodName
          ;

        correctedQuery = buildSearchString( obj.queryString );
        if(model.searchOptions.selectedSearchType === model.langConstants.JAVA_SCRIPT){
          token = 'types.typeName';
          methodName = '/typejs';
        }else if(model.searchOptions.selectedSearchType === model.langConstants.SCALA){
          token = 'tokens.importName';
          methodName = '/typescala';
        }else{
          token = 'tokens.importName';
          methodName = '/typeimportsmethods';
        }
        queryBlock = getQuery(correctedQuery, token, 'must');
        queryES(
          {
            indexName: 'importsmethods',
            methodName:methodName,
            queryBody: {
              'query': queryBlock,
              'sort': [{
                'score': {
                  'order': 'desc'
                }
              }]
            },
            callbackObj: {
              correctedQuery: correctedQuery,
              query: obj.queryString
            },

            resultSize: obj.resultSize || settings.resultSize,
            callback: obj.callback
          }
        );

      }

      function getQuery( queryString, token, key ) {
        var terms = queryString.split( ',' ),
          mustTerms = terms.map( function( queryTerm ) {
              var prefix = ( queryTerm.search( /\*/ ) >= 0 || queryTerm.search( /\?/ ) >= 0 ) ? 'wildcard' : 'term';
              var result = {};
              result[ prefix ] = {
              };
              result[ prefix ][ token ] =  queryTerm.trim().toLowerCase();
              return result;
          } );
        var returnObj = {};
        returnObj.bool = {};
        returnObj.bool[ key ] = mustTerms;
        if( key === 'should' ) {
          returnObj.bool.must = [];
        } else {
          returnObj.bool.should = [];
        }
        return returnObj;
      }

      function queryES( obj ) { // indexName, queryBody, resultSize, successCallback
        var url = settings.esURL
                  + '/'
                  + obj.indexName
                  + obj.methodName
                  + '/_search?size='
                  + ( obj.resultSize || 50 )
                  + '&source='
                  + JSON.stringify( obj.queryBody )
                  ;

        http.get(url)
          .then(function( result ) {  

            obj.callbackObj = obj.callbackObj || {};
            obj.callbackObj.totalHitCount = result.hits.total;
            if(model.searchOptions.selectedSearchType === model.langConstants.JAVA_SCRIPT){
              var jsData = restructureData(result.hits.hits);
              obj.callbackObj.result = jsData;
            }else{
              obj.callbackObj.result = result.hits.hits;
            }            
            obj.callbackObj.status = 'success';
            obj.callback( obj.callbackObj );


          }, function(error, s) {

            obj.callbackObj = obj.callbackObj || {};
            obj.callbackObj.result = error;
            obj.callbackObj.status = 'error';
            obj.callback( obj.callbackObj );

          } );
      }

      function restructureData(hits){
          var hitsData = angular.copy(hits);
          _.forEach(hitsData,function(eachHit){
              eachHit._source.tokens = eachHit._source.types;
              _.forEach(eachHit._source.tokens,function(eachToken){
                  eachToken.importExactName = eachToken.typeName;
                  eachToken.importName = eachToken.typeName;
                  _.forEach(eachToken.properties,function(eachProperty){
                      eachProperty.methodName = eachProperty.propertyName;
                      delete eachProperty.propertyName;
                  });
                  eachToken.methodAndLineNumbers = eachToken.properties;
                  delete eachToken.typeName;
                  delete eachToken.properties;
              });
              delete eachHit._source.types;
          });
          return hitsData;
      }

      function renderFileContent(files, callback) {
         var content;

          var obj = {
            indexName: 'sourcefile',
            methodName:'',
            queryBody: fetchFileQuery(files),
            callbackObj: {
              files: files
            },
            resultSize: 50,
            callback: callback
          }
          queryES(obj);
      }

      function fetchFileQuery(files) {
        var arr=[];

        for( var i=0; i < files.length ; i++ ) {
          arr.push( {
            'term' : {
              'fileName': files[i].path
            }
          } )
        }

        return {
          'query': {
            'bool': {
              'should' : arr
            }
          }
        };
      }

      function getFilteredFiles( data, pkgs ) {

        var result = angular.copy( data ) || [];
        if(  pkgs ) {

          for( var pkg in  pkgs ) {

            var pkgItem = pkgs[ pkg ];

            result = _.map( result, function( r ) {
              if( r.fileMatchingImports[ pkg ] ) {
                return r;
              }
            } );

            result = _.remove(result, undefined );

            for( var m in pkgItem.methods ) {

              result = _.map( result, function( r ) {
                if( r.fileMatchingImports[ pkg ].indexOf( m ) !== -1 ) {
                  return r;
                }
              } );
              result = _.remove( result, undefined );
            }
          }
        }

        return result;
      }


      function groupByFilename ( data ) {

          var groupedData = _.groupBy(data, function(entry) {
            return entry._source.file;
          });
          return groupedData;
      }

      function groupByImportsAndFile ( obj ) {

        var matchingImports= {};

        intermediateResult = _.map(obj.data, function(files, fileName) {

          var labels = getFileName(fileName),
            lineNumbers = [],
            fileMatchingImports = {},
            matchedMethodLines = {},
            matchedImportLines = {};

          files.forEach(function(f) {
            var matchingTokens = filterRelevantTokens(obj.searchString.toLowerCase(), f._source.tokens)
                ;


            matchingTokens.map(function(x) {
              matchingImports[x.importExactName] = matchingImports[x.importExactName] || {
                methodCount : 0,
                methods: [],
                importName: x.importExactName
              };
              matchingImports[x.importExactName].methodCount++;
              matchingImports[x.importExactName].methods = matchingImports[x.importExactName].methods.concat(x.methodAndLineNumbers.map(function(m) {
                return 'm_' + m.methodName
              }));
              fileMatchingImports[x.importExactName] = fileMatchingImports[x.importExactName] || [];
              fileMatchingImports[x.importExactName] = fileMatchingImports[x.importExactName].concat(x.methodAndLineNumbers.map(function(m) {
                return 'm_' + m.methodName
              }));

              matchedImportLines[ x.importExactName ] = matchedImportLines[ x.importExactName] || [];
              matchedImportLines[ x.importExactName] = matchedImportLines[ x.importExactName].concat( x.lineNumbers );

              x.methodAndLineNumbers.map( function( m ) {
                
                matchedMethodLines[ 'm_' + m.methodName ] = matchedMethodLines[ 'm_' + m.methodName ] || [];
                matchedMethodLines[ 'm_' + m.methodName ] = matchedMethodLines[ 'm_' + m.methodName ].concat( m.lineNumbers );

              } );
            });
          });


          fileMatchingImports.methodCount = 0;

          _.each(fileMatchingImports,function( methods, name ){
            methods = _.unique( methods );
            if( name !== 'methodCount' ) {
              fileMatchingImports.methodCount += methods.length
            }
          });

          return {
            path: fileName,
            repo: labels.repo,
            name: labels.file,
            score: files[0]._source.score,
            fileMatchingImports: fileMatchingImports,
            matchedMethodLines: matchedMethodLines,
            matchedImportLines: matchedImportLines
          };
        } );

        return {
          classes: matchingImports,
          result: intermediateResult
        };
      }


      function getFileName(filePath) {
        var elements = filePath.split('/'),
          repoName = elements[0] + '-' + elements[1],
          fileName = elements[elements.length - 1];
        return {
          'repo': repoName,
          'file': fileName
        };
      }



      function filterRelevantTokens( searchString, tokens ) {

        var result = searchString.split( ',' ).map( function( term ) {
          var matchingTokens = [],
              correctedTerm = term.trim().replace( /\*/g, '.*' ).replace( /\?/g, '.{1}' );

          matchingTokens = tokens.filter( function( tk ) {
              return ( tk.importName ).search( correctedTerm ) >= 0;
          } );
          return matchingTokens;
        } );
        return _.flatten( result );

      }


      function sanitizeLastChar ( obj ) {
        if( obj.content[ obj.content.length -1 ] === '\n' ) {
          obj.end--;
          obj.endIndex--;
          obj.content = obj.content.substring( 0, obj.content.length -1 )
        }
      }
      function getLineData( content, lastObj, line, offset, obj ) {
        var l = line.lineNumber;
        if( l-offset - 1 < 0 ) {
          l = offset + 1;
        }
        if( obj.length ) {

          var lastObj = obj[ obj.length -1 ];
          if( lastObj.state ) {
            if( lastObj.end + offset >= l ) {
              var i2 = getPosition( content, '\n', l + offset );
              if( i2 === -1 ) {
                i2 = getPosition( content, '\n', l + offset - 1 );
              }
              lastObj.content +=  content.substring( lastObj.endIndex, i2 ) ;
              lastObj.endIndex = i2 ;
              lastObj.end = l +  offset + 1;
              lastObj.lineNumbers.push( line );
              sanitizeLastChar( lastObj );
            } else {
                var i1 = getPosition( content, '\n', l - offset -1 );
                obj.push( {
                  start: lastObj.end - 1,
                  end: l - offset - 1,
                  content: content.substring( lastObj.endIndex+1, i1 ) ,
                  state: false,
                  startIndex: 0,
                  endIndex: i1
                } );
                sanitizeFirstChar( obj[ obj.length -1 ] );
                sanitizeLastChar( obj[ obj.length -1 ] );
              var i2 = getPosition( content, '\n', l + offset );
              if( i2 === -1 ) {
                i2 = getPosition( content, '\n', l + offset - 1 );
              }
               obj.push( {
                start: l - offset - 1 ,
                end: l + offset + 1,
                content: content.substring( i1, i2 ).substring(1) ,
                state: true,
                startIndex: i1,
                endIndex: i2,
                lineNumbers: [ line ]
              } );
              sanitizeLastChar( obj[ obj.length -1 ] );
              sanitizeFirstChar( obj[ obj.length -1 ] );
            }
          }


        } else {
          var i1 = getPosition( content, '\n', l - offset - 1  );
          if( l !== offset + 1 ) {
              obj.push( {
              start: 0,
              end: l - offset - 1,
              content: content.substring( 0, i1 )  ,
              state: false,
              startIndex: 0,
              endIndex: i1
            } );
            sanitizeFirstChar( obj[ obj.length -1 ] );
            sanitizeLastChar( obj[ obj.length -1 ] );  
          } else {
            i1 = 0;
          }
          
          var i2 = getPosition( content, '\n', l + offset );
          if( i2 === -1 ) {
            i2 = getPosition( content, '\n', l + offset - 1 );
          }
          var cont = content.substring( i1, i2 ) ;
          if( i1 !== 0) {
            cont = cont.substring(1);
          } 

          obj.push( {
            start: l - offset - 1,
            end: l + offset + 1,
            content: cont,
            state: true,
            startIndex: i1,
            endIndex: i2,
            lineNumbers: [ line ]
          } );
          sanitizeLastChar( obj[ obj.length -1 ] );
          sanitizeFirstChar( obj[ obj.length -1 ] );


        }
      }

      function getPosition(fileContent, splitChar, lineNumber) {
         return fileContent.split(splitChar, lineNumber).join(splitChar).length;
      }

      function nth_occurrence (string, char1, nth) {
        var first_index = string.indexOf(char1);
        var length_up_to_first_index = first_index + 1;

        if (nth <= 1) {
            return first_index;
        } else {
            var string_after_first_occurrence = string.slice(length_up_to_first_index);

            var next_occurrence = nth_occurrence(string_after_first_occurrence, char1, nth - 1);

            if (next_occurrence === -1) {
                return -1;
            } else {
                return length_up_to_first_index + next_occurrence;
            }
        }
      }

      function sanitizeFirstChar ( obj ) {
        if( obj.content[0] === '\n' ) {
          obj.content = ' ' + obj.content;
        }
      }

      function getlinesObj( fileContent, lines, offset ) {

        var obj = [];
        var str = '';
        var l1 = 0;
        var l2;
        var count = 0;
        var i1=0;
        var i2;
        var lastObj;

        for( var k = 0; k < lines.length ; k++  ) {
          getLineData( fileContent, lastObj, lines[k], offset, obj );
        }
        if( obj.length ) {
          var lastObj = obj[ obj.length - 1  ]
          obj.push( {
              start: lastObj.end - 1,
              end:-1,
              content: fileContent.substring( lastObj.endIndex+1 ),
              state: false,
              startIndex: 0,
              endIndex: -1
          } )
        }

        return obj;
      }

      function updatedLineNumbers( file, pkgs ) {

        var filterExcuted = false;
        if(  pkgs ) {
          file.lines = [];
          for( var pkg in  pkgs ) {
            filterExcuted = true;
            var pkgItem = pkgs[ pkg ];
            if(pkgItem.status) {
              file.lines = file.lines.concat( file.matchedImportLines[ pkg ] );
            }
            for( var m in pkgItem.methods ) {
              file.lines = file.lines.concat(file.matchedMethodLines[ m ]);
            }

          }
        }
        if( !filterExcuted ) {
          allLines( file );
        }
        file.lines = uniqueAndSortLines( file.lines );
      }

      function allLines( file ) {
        file.lines = [];
        _.each(file.matchedImportLines, function ( x ) {
          file.lines = file.lines.concat( x );
        } );

        _.each(file.matchedMethodLines, function ( x ) {
          file.lines = file.lines.concat( x );
        } );

      }

      function uniqueAndSortLines( lines ) {
        return (_.unique(_.flatten(lines))).sort(function(a, b) {
          return a.lineNumber - b.lineNumber;
        });
      }


      function splitFileContent ( fileContent, fileInfo, offset ) {
        return {
          content: fileContent,
          fileInfo: fileInfo,
          linecount: fileContent.split( '\n' ).length + 1,
          linesData: getlinesObj( fileContent, fileInfo.lines, offset )
        }
      }

      var searchRepotopic = function  ( obj ) {
        
        var correctedQuery
          , queryBlock
          ;

        correctedQuery = buildSearchString( obj.queryString );
        queryBlock = getQuery(correctedQuery, 'terms.term', 'must' );

        queryES(
          {
            indexName: 'repotopic',
            methodName:'/typerepotopic',
            queryBody: {
              'query': queryBlock,
              "from": 0,
              "size": 10
            },
            callbackObj: {
            },
            resultSize: obj.resultSize || settings.resultSize,
            callback: obj.callback
          } );
      };

      return {
        search: search,
        getFilteredFiles: getFilteredFiles,
        config: function( obj ) {
          settings = obj
        },
        groupByFilename: groupByFilename,
        groupByImportsAndFile: groupByImportsAndFile,
        renderFileContent: renderFileContent,
        splitFileContent: splitFileContent,
        setData: function  ( key, value ) {
          this[ key ] = value;
        },
        getData: function  ( key ) {
          return this[ key ];
        },
        updatedLineNumbers: updatedLineNumbers,
        searchRepotopic: searchRepotopic
      };
    }
  ]);

})( KB.module );
