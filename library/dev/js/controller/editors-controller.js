(function( module ) {
	module.controller('editorCtroller', [
    '$scope',
    'model',
    'docsService',
    '$location',
    '$timeout',
    function($scope, model, docsService,$location,$timeout) {


      $scope.model = model;

      $scope.tabs = [{
        active: false,
        heading: 'Files'
      }, {
        active: true,
        heading: 'Methods'
      }];

      $scope.calcTop = function( snipp, line ) {
        var val = line.lineNumber - snipp.start;
        var totHeight = $(snipp.ele).closest( '.snippet-holder' ).height();
        var offset = $(snipp.ele)[0].offsetTop + snipp.ele.find( 'ol li:nth-child(' +val + ')'  )[0].offsetTop;
        return {
          top: (offset/totHeight)*100 + '%'
        }
      };

      $scope.relatedTypeOnClick = function(relatedType) {
          var reqObj = { term: relatedType, type: "type" };
          selectedTexts.push(reqObj);
      }

      $scope.goToTop = function(snipp, line) {
        var val = line.lineNumber - snipp.start;
        var scrollEle = $(snipp.ele).closest( '.snippet-wraper' );
        var offset = $(snipp.ele)[0].offsetTop + snipp.ele.find( 'ol li:nth-child(' +val + ')'  )[0].offsetTop;
        scrollEle.scrollTop(offset);
      }

      $scope.toggleCode =  function( snipp ) {
        snipp.show=!snipp.show;
        /*hack to adjust the markers position*/
        $timeout( function() {
        }, 100);
      };

      $scope.toggleExpand = function(e, index, item) {
        e.preventDefault();
        e.stopPropagation();
        var currentTarget= e.currentTarget;
        setTimeout( function(){
          $(window).scrollTop($(currentTarget).offset().top - 10);
        } );

        item.collapsed = !item.collapsed;

        item.showBody = item.collapsed;
        if( model.expndedView ) {
          item.showBody = true;
        }
        for( var i in item.linesData ) {
          item.linesData[i].show = item.collapsed;
        }
        /*hack to adjust the markers position*/
        $timeout( function() {
        }, 100);
      };


      $scope.toggleCodeSnippets = function( e ) {
        e.preventDefault();
        model.toggelSnippet = !model.toggelSnippet;
        for(var i=0;i<model.editors.length;i++) {
          model.editors[i].showBody = model.toggelSnippet;
          model.editors[i].collapsed = false;
        }
      }

      $scope.fullExpandView = function(e, item, index) {

        e.preventDefault();
        var currentTarget= e.currentTarget;
        setTimeout( function(){
          $(window).scrollTop($(currentTarget).offset().top -10 );
        } );
        $scope.hideLeftPanel = !$scope.hideLeftPanel;
        if (!$scope.hideLeftPanel) {
          model.expandedItem = false;
          model.expndedView = false;
        } else {
          model.expandedItem = item;
          model.expandedItem.showBody = true;
          model.expndedView = true;
        }
      };

      $scope.mode = 'java';

      $scope.updateFilter = function() {

          var search = $location.search();
          var pkgs = angular.copy(model.packages);
          var innerQuery;

          var types = [];

          for(var p in pkgs ) {
            innerQuery = [];

            for(var m in pkgs[ p ].methods ) {
              if( pkgs[p].methods[m] ) {
                innerQuery.push( m );
                types.push({type:'method', term: p+'.'+m+'()'});
              } else {
                delete pkgs[p].methods[m];
              }
            }
            if( !innerQuery.length && !pkgs[p].status ) {
              delete pkgs[ p ];
            }
          }
          model.filterSelected = true;
          if (types.length == 0) {
            var terms = Object.keys(model.packages);
            terms.forEach(function(term){
              types.push({type:'type', term: term});
            });
          }
          search.searchTerms = JSON.stringify(types);
          search.filter = JSON.stringify( model.packages );
          $location.search( search  );
      };

      $scope.loadRepos = function () {

        if( model.repos || model.repoRequest === 'sent' ) {
          return;
        }
        var selectedTexts = $location.search().searchTerms;
        model.repos = false;
        model.repoRequest = 'sent';
        docsService.searchRepotopic( {
          queryString: selectedTexts,
          callback: function  ( obj ) {
            model.repoRequest = 'responded';
            var index;
            var repos = [];
            var obj;
            var url;
            var sortedArray;
            var res = obj.result;
            for( var i=0; i < res.length ; i++ ) {
              obj = { score: 0 };
              addrepo  = false;
              for( var j =0; j< res[i]._source.terms.length; j++ ) {
                if( model.selectedTexts.indexOf( res[i]._source.terms[j].term ) !== -1 ) {
                  addrepo = true;
                  /*obj.terms = obj.terms || [];
                  obj.terms.push( {
                    term: res[i]._source.terms[j].term,
                    score: res[ i ]._source.terms[ j ].score
                  } );*/

                  obj.score += res[i]._source.terms[j].score;
                }
              }


              if( addrepo ) {

                obj.files = res[ i ]._source.files.slice( 0 , 5 );
                if( obj.files.length ) {
                  obj.reponame = ( function ( url ) {
                    url = url.split('/');
                    return url[0] + '/' + url[1];
                  } )( obj.files[0].fileName );
                  obj.terms = res[i]._source.terms.slice( 0, 10 );
                  obj.repoStars = +res[i]._source.repository.repoStars;
                  obj.repoId = res[i]._source.repository.repoId;
                  repos.push( obj );
                }


              }
            }
            model.repos = repos;
          }
        } );
      };

      $scope.changeTab = function( activeTab ) {

        if(model.searchOptions.selectedSearchType === model.langConstants.JAVA_SCRIPT){
          return;
        }
        model.tab.files= false;
        model.tab.repos= false;
        model.tab[ activeTab ] = true;

        var search = $location.search();
        search.activeTab = activeTab;
        $location.search( search );
      };

      $scope.orderByKey = '-score';

      $scope.repoSortBy = function( key ) {
        if( key === 'score' ) {
          $scope.orderByKey = $scope.orderByKey === '-score' ? 'score' : '-score';
        }
        if( key === 'repoStars' ) {
          $scope.orderByKey = $scope.orderByKey === '-repoStars' ? 'repoStars' : '-repoStars';
        }
      };

    }
  ])
.filter('repoFileName', function() {
  return function(input) {
    input = input.split('/');
    return input[input.length-1];
  };
});

})( KB.module )
