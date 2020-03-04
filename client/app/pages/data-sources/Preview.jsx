import { react2angular } from 'react2angular';
import { routesToAngularRoutes } from '@/lib/utils';
import React from 'react';
import { MDBDataTable } from 'mdbreact';
import { $route } from '@/services/ng';
import { PageHeader } from '@/components/PageHeader';


class PreviewTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      isLoaded: false,
      data: {},
      error: null,
    };
  }

  componentDidMount() {
    fetch(`api/data_sources/${$route.current.params.dataSourceId}/preview/${$route.current.params.tableName}`)
      .then(data => data.json())
      .then((result) => {
        if (result.preview.error) {
          this.setState({
            isLoaded: true,
            error: result.preview.error,
          });
        } else {
          this.setState({
            isLoaded: true,
            data: result.preview,
          });
        }
      }, (error) => {
        this.setState({
          isLoaded: true,
          error,
        });
      });
  }

  render() {
    const { isLoaded, data, error } = this.state;
    const tableName = $route.current.params.tableName;

    if (isLoaded) {
      if (error) {
        return (
          <div className="container">
            Error: {error.message}
          </div>
        );
      }
      return (
        <div className="container">
          <PageHeader title={tableName} />
          <MDBDataTable
            striped
            bordered
            hover
            data={data}
          />
        </div>
      );
    }

    return (
      <div className="container">
        Loading...
      </div>
    );
  }
}

export default function init(ngModule) {
  ngModule.component('pagePreviewTable', react2angular(PreviewTable));

  return routesToAngularRoutes([
    {
      path: '/preview/:dataSourceId/:tableName',
      title: 'Preview',
      key: 'preview',
    },
  ], {
    reloadOnSearch: false,
    template: '<page-preview-table on-error="handleError"></page-preview-table>',
    controller($scope, $exceptionHandler) {
      'ngInject';

      $scope.handleError = $exceptionHandler;
    },
  });
}

init.init = true;
