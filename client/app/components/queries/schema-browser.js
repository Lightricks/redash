import template from './schema-browser.html';

function SchemaBrowserCtrl($rootScope, $scope) {
  'ngInject';


  this.showTable = (table) => {
    table.collapsed = !table.collapsed;
    $scope.$broadcast('vsRepeatTrigger');
  };

  this.getSize = (table) => {
    let size = 22;

    if (!table.collapsed) {
      size += 18 * table.columns.length;
    }

    return size;
  };

  this.isEmpty = function isEmpty() {
    return this.schema === undefined || this.schema.length === 0;
  };

  this.itemSelected = ($event, hierarchy) => {
    $rootScope.$broadcast('query-editor.command', 'paste', hierarchy.join('.'));
    $event.preventDefault();
    $event.stopPropagation();
  };

  this.preview_table = ($event, tableName) => {
    $event.preventDefault();
    $event.stopPropagation();

    window.open(
      '/preview/' + $scope.$ctrl.dsId + '/' + tableName,
      '_blank',
      'height=1000,width=1000,menubar=0,toolbar=0',
      false,
    );
  };

  this.splitFilter = (filter) => {
    filter = filter.replace(/ {2}/g, ' ');
    if (filter.includes(' ')) {
      const splitTheFilter = filter.split(' ');
      this.schemaFilterObject = { name: splitTheFilter[0], columns: splitTheFilter[1] };
      this.schemaFilterColumn = splitTheFilter[1];
    } else {
      this.schemaFilterObject = filter;
      this.schemaFilterColumn = '';
    }
  };
}

const SchemaBrowser = {
  bindings: {
    schema: '<',
    onRefresh: '&',
    preview: '<',
    dsId: '<',
  },
  controller: SchemaBrowserCtrl,
  template,
};

export default function init(ngModule) {
  ngModule.component('schemaBrowser', SchemaBrowser);
}

init.init = true;
