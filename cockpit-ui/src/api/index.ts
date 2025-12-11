/* eslint-disable @typescript-eslint/explicit-module-boundary-types */

const cockpitPrefix = '/api/v1/';

const api = {
  // 业务分类
  dataMart: cockpitPrefix + 'data-modal/data-mart',
  dataMartTree: cockpitPrefix + 'data-modal/data-mart/tree',
  dataMartList: cockpitPrefix + 'data-modal/data-mart/list',
  dataMartAll: cockpitPrefix + 'data-modal/data-mart/all',

  // 业务分类
  subjectDomain: cockpitPrefix + 'data-modal/subject-domain',
  subjectDomainTree: cockpitPrefix + 'data-modal/subject-domain/tree',
  subjectDomainList: cockpitPrefix + 'data-modal/subject-domain/list',

  businessProces: cockpitPrefix + 'data-modal/business-process',
  businessProcesList: cockpitPrefix + 'data-modal/business-process/list',

  dataDomain: cockpitPrefix + 'data-modal/data-domain',
  dataDomainList: cockpitPrefix + 'data-modal/data-domain/list',

  businessType: cockpitPrefix + 'data-modal/business_type',
  treeBusinessType: cockpitPrefix + 'data-modal/business_type/tree',
  listBusinessType: cockpitPrefix + 'data-modal/business_type/list',
  businessTypeAll: cockpitPrefix + 'data-modal/business_type/all',
  businessTypeData: cockpitPrefix + 'data-modal/business_type/data',
  businessTypeDataList: cockpitPrefix + 'data-modal/business_type/data/list',

  // 业务分类
  getWarehouseLayer: cockpitPrefix + 'data-modal/warehouse-layer',
  getWarehouseLayerList: cockpitPrefix + 'data-modal/warehouse-layer/list',

  dataStdFolder: cockpitPrefix + 'data-modal/field-std/folder',
  dataStdFolderTree: cockpitPrefix + 'data-modal/field-std/folder/tree',
  dataStdAllList: cockpitPrefix + 'data-modal/field-std/list/all',
  dataStd: cockpitPrefix + 'data-modal/field-std',


  dataCodeFolder: cockpitPrefix + 'data-modal/code-std/folder',
  dataCode: cockpitPrefix + 'data-modal/code-std',

  dataUnitFolder: cockpitPrefix + 'data-modal/unit-std/folder',
  dataUnit: cockpitPrefix + 'data-modal/unit-std',

  timeGranularity: cockpitPrefix + 'data-modal/time-granularity',
  

  indicatorModifier: cockpitPrefix + 'indicator/modifier',


  indicatorAtom: cockpitPrefix + 'indicator-atom',

  indicatorDerive: cockpitPrefix + 'indicator/derive',

  indicatorCompose: cockpitPrefix + 'indicator/compose',

  modeTable: cockpitPrefix + 'mode-table',

  dimension: cockpitPrefix + 'dimension',

  datasource: cockpitPrefix + 'datasource',

  taskDefinition: cockpitPrefix + 'task-definition',
  taskInstance: cockpitPrefix + 'task-instance',
  
};

export default api;
