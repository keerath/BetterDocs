(function( module ) {

	module
  .factory('docsService', [
    'http','model', '$http', '$interval',
    function(
      http,model,$http, $interval
    ) {

      var settings, editors = [], editorIndex = 0, editorInterval;

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
              return ( tk.typeName ).search( correctedTerm ) >= 0;
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


      function splitFileContent ( fileObj, offset ) {
        return {
          content: fileObj.content,
          fileInfo: fileObj.fileInfo,
          linecount: fileObj.content.split( '\n' ).length + 1,
          linesData: getlinesObj( fileObj.content, fileObj.fileInfo.lines, offset )
        }
      }

      function searchRequest(selectedTexts) {
        var typeReq = selectedTexts;
        var wordReq = '';
        typeReq = typeReq.filter(function(text){
          if(text.type == 'word'){
            wordReq += ' '+text.term;
            return false;
          }
          else {
            return true;
          }
        });
        if (wordReq) {
          typeReq.push({type:'word', term: wordReq});
        }
        var searchRequest = {
          "queries": typeReq,
          "from": model.currentPageNo * 10,
          "size": 10
        };
        var searchRequest = JSON.stringify(searchRequest);
        searchRequest = searchRequest.replace(/\(\)/,'');
        searchTypes(searchRequest);
      }

      function searchTypes(searchRequest) {
        model.editors = [], editors = [];
        model.emptyResponse = false;
        model.filterNotFound = false;

        $http.get(settings.esURL + '/search?query='+searchRequest)
        .then(searchResultHandler , searchErrorHandler);
      }

      function searchResultHandler(result) {
        if (result && result.data && result.data.hits.length === 0) {
          model.emptyResponse = true;
          return;
        }
        model.showPageResponse = true;
        model.totalFiles = result.data.total_hits;
        model.totalHitCount = result.data.total_hits;
        model.relatedTypes = result.data.related_types;
        var linesObj, imports = {};

        result.data.hits.forEach(function(eachFile){
          var lines = [];
          eachFile.types.forEach(function(eachType){
            imports[eachType.name] = eachType.name;
            eachType.props.forEach(function(eachProp){
              eachProp.lines.forEach(function(eachLine){
                linesObj = {'lineNumber':eachLine[0], 'columnStart':eachLine[1], 'columnEnd':eachLine[2]};
                lines.push(linesObj);
              });
            });
          });
          var sortedLines = uniqueAndSortLines(lines);
          var labels = getFileName(eachFile.fileName);
          var fileObj = {
            'content':eachFile.fileContent,
            'fileInfo': {
              'lines': sortedLines,
              'repo': labels.repo,
              'name': labels.file,
              'path': eachFile.fileName
            }
          };
          var editorContent = splitFileContent( fileObj, model.config.offset || 2 );
          editors.push(editorContent);
        });

        editorIndex = 0;
        editorInterval = $interval(renderEditor,200);

        if( !model.filterSelected ) {
          $http.get(settings.esURL + '/properties?types=' + JSON.stringify(Object.keys(imports)))
          .then(importsHandler);
        }

        /*var editors = [];
        for (var i=0; i<linesData.length; i++) {
          (function (n) {
            $timeout(function() {
              model.editors.push(splitFileContent( linesData[n].content, linesData[n].fileInfo, model.config.offset || 2 ))
              console.log('Editor : ' + n);
            }, 0);
          })(i);
        }*/
      }

      function renderEditor() {
        console.log('Inside interval : '+editorIndex);
        if (editorIndex < editors.length) {
          model.editors.push(editors[editorIndex]);
          editorIndex++;
        } else {
          $interval.cancel(editorInterval);
          editorInterval = undefined;
        }
      }

      function searchErrorHandler(error) {
        model.emptyResponse = true;
      }

      function importsHandler(res) {
        model.groupedMethods = res.data;
      }

      return {
        searchRequest: searchRequest,
        getFilteredFiles: getFilteredFiles,
        config: function( obj ) {
          settings = obj
        },
        setData: function  ( key, value ) {
          this[ key ] = value;
        },
        getData: function  ( key ) {
          return this[ key ];
        },
        updatedLineNumbers: updatedLineNumbers,
        fileName: getFileName,
        uniqueAndSortLines: uniqueAndSortLines
      };
    }
  ]);

})( KB.module );
