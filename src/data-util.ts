import * as mingo from 'mingo';
import * as math from 'mathjs';
import * as ObjUtil from '@nervmouse/object-util';



// Type definitions
type AnyObject = Record<string | number, any>;
type MongoQuery = Record<string, any>;
type SortOptions = Record<string, 1 | -1>;
type ComputedField = string;
type FieldPath = string;
type GroupByField = string;
type PickOneOperation = 'first' | 'last' | number;

interface ComputedConfig {
  require?: {[key:string]:any};
  data_scope?: string;
  inplace?: boolean;
}

interface ComputedFields {
  $config?: ComputedConfig;
  [key: string]: ComputedField | ComputedConfig | undefined;
}

interface GroupByOptions {
  field: GroupByField;
  remove_field?: boolean;
  pick_one?: PickOneOperation;
  order?: SortOptions;
}

interface ToMapOptions {
  field: string;
  multiple?: boolean;
}

interface ProcessOptions {
  agg?: any[];
  query?: MongoQuery;
  computed?: ComputedFields;
  sort?: SortOptions;
  fields?: FieldPath[];
  exclude_fields?: FieldPath[];
  group_by?: GroupByField | GroupByOptions;
  to_map?: string | ToMapOptions;
  format?: 'csv';
}

interface DataUtilFunctions {
  query: (data: AnyObject[], opts: MongoQuery) => AnyObject[];
  agg: (data: AnyObject[], pipelines: any[]) => AnyObject[];
  sort: (data: AnyObject[], opts: SortOptions) => AnyObject[];
  computed: (data: AnyObject[], computed: ComputedFields) => AnyObject[];
  select: (data: AnyObject[], fields?: FieldPath[], exclude_fields?: FieldPath[]) => AnyObject[];
  groupBy: (data: AnyObject[], field: GroupByField, remove_field?: boolean, pick_one?: PickOneOperation, order?: SortOptions) => Record<string, AnyObject | AnyObject[]>;
  pickOne: (data: AnyObject[], oper: PickOneOperation) => AnyObject | undefined;
  toMap: (data: AnyObject[], col: string, multiple?: boolean) => Record<string, AnyObject | AnyObject[]>;
  toCSV: (data: AnyObject[]) => string;
}

interface OperatorFunctions {
  first: (arr: AnyObject[]) => AnyObject | undefined;
  last: (arr: AnyObject[]) => AnyObject | undefined;
}

const fns: DataUtilFunctions = {
  query(data: AnyObject[], opts: MongoQuery): AnyObject[] {
    const MongoQ = new mingo.Query(opts);
    const cursor = MongoQ.find(data);
    return cursor.all() as AnyObject[];
  },

  agg(data: AnyObject[], pipelines: any[]): AnyObject[] {
    return mingo.aggregate(data, pipelines) as AnyObject[];
  },

  sort(data: AnyObject[], opts: SortOptions): AnyObject[] {
    const MongoQ = new mingo.Query({});
    const cursor = MongoQ.find(data);
    cursor.sort(opts);
    return cursor.all() as AnyObject[];
  },

  computed(data: AnyObject[], computed: ComputedFields): AnyObject[] {
    const cmpArr: Array<{ evaluate: (scope: AnyObject) => any; prop: string }> = [];
    const env: AnyObject = {};
    let scope_name: string | undefined;
    let cfg_inplace = true;

    for (const prop in computed) {
      if (prop === '$config') {
        const config = computed['$config'] as ComputedConfig;
        const { require: packages, data_scope, inplace } = config;
        
        if (inplace !== undefined) {
          cfg_inplace = inplace;
        }
        scope_name = data_scope;
        
        if (packages) {
          for (const pkgName in packages) {
            const pkg = packages[pkgName];
            if(typeof pkg ==='object'){
                env[pkgName]=pkg
            }else if(typeof pkg ==='function'){
                env[pkgName] =pkg()
            }else if(typeof pkg ==='string'){
                console.log(`dynamic require ${pkgName} is not supported`)
            }
            
          }
        }
      } else {
        const fieldExpression = computed[prop] as ComputedField;
        const cmp = math.compile(fieldExpression);
        (cmp as any).prop = prop;
        cmpArr.push(cmp as any);
      }
    }

    const new_data: AnyObject[] = [];
    
    for (const d of data) {
      let new_d: AnyObject = {};
      
      for (const cmp of cmpArr) {
        try {
          let scope: AnyObject;
          if (scope_name) {
            scope = { ...env, [scope_name]: d };
          } else {
            scope = { ...env, ...d };
          }
          
          const val = cmp.evaluate(scope);
          
          if (cfg_inplace) {
            ObjUtil.objPathSet(d, cmp.prop, val, true);
            new_data.push(d);
          } else {
            ObjUtil.objPathSet(new_d, cmp.prop, val, true);
            new_data.push(new_d);
          }
        } catch (e) {
          console.log(e);
        }
      }
    }
    
    return new_data;
  },

  select(data: AnyObject[], fields?: FieldPath[], exclude_fields?: FieldPath[]): AnyObject[] {
    const new_data: AnyObject[] = [];
    
    if (!fields && !exclude_fields) {
      return data;
    }
    
    for (const rd of data) {
      let d: AnyObject = {};
      
      if (fields) {
        for (const field of fields) {
          ObjUtil.objPathSet(
            d,
            field,
            ObjUtil.objPath(rd, field),
            true
          );
        }
      } else {
        Object.assign(d, rd);
      }
      
      if (exclude_fields) {
        for (const field of exclude_fields) {
          ObjUtil.objPathDelete(d, field);
        }
      }
      
      new_data.push(d);
    }
    
    return new_data;
  },

  groupBy(
    data: AnyObject[], 
    field: GroupByField, 
    remove_field?: boolean, 
    pick_one?: PickOneOperation, 
    order?: SortOptions
  ): Record<string, AnyObject | AnyObject[]> {
    const map: Record<string, AnyObject[]> = {};
    
    for (const d of data) {
      const key = d[field] as string;
      if (!map[key]) {
        map[key] = [];
      }
      
      if (remove_field) {
        delete d[field];
      }
      
      map[key].push(d);
    }

    if (order) {
      for (const key in map) {
        const sortOrder = {
          asc: [] as string[],
          desc: [] as string[]
        };
        
        for (const orderKey in order) {
          if (order[orderKey] === 1) {
            sortOrder.asc.push(orderKey);
          } else if (order[orderKey] === -1) {
            sortOrder.desc.push(orderKey);
          }
        }
        
        // desc first
        for (const descKey of sortOrder.desc) {
          map[key] = map[key].sort((a, b) => {
            return order[descKey] * (new Date(a[descKey]).getTime() - new Date(b[descKey]).getTime());
          });
        }
        
        for (const ascKey of sortOrder.asc) {
          map[key] = map[key].sort((a, b) => {
            return order[ascKey] * (new Date(a[ascKey]).getTime() - new Date(b[ascKey]).getTime());
          });
        }
      }
    }

    if (pick_one) {
      const result: Record<string, AnyObject | AnyObject[]> = {};
      for (const key of Object.keys(map)) {
        const arr = map[key];
        result[key] = fns.pickOne(arr, pick_one);
      }
      return result;
    }

    return map;
  },

  pickOne(data: AnyObject[], oper: PickOneOperation): AnyObject | undefined {
    const operFn: OperatorFunctions = {
      first(arr: AnyObject[]): AnyObject | undefined {
        return arr[0];
      },
      last(arr: AnyObject[]): AnyObject | undefined {
        if (arr.length > 0) {
          return arr[arr.length - 1];
        } else {
          return undefined;
        }
      }
    };

    const operType = typeof oper;
    if (operType === 'string') {
      return operFn[oper as keyof OperatorFunctions](data);
    } else if (operType === 'number') {
      return data[oper as number];
    }
    
    return undefined;
  },

  toMap(data: AnyObject[], col: string, multiple = false): Record<string, AnyObject | AnyObject[]> {
    const map: Record<string, AnyObject | AnyObject[]> = {};
    
    if (multiple) {
      for (const d of data) {
        const key = d[col] as string;
        if (map[key]) {
          if (!Array.isArray(map[key])) {
            map[key] = [map[key] as AnyObject];
          }
          (map[key] as AnyObject[]).push(d);
          continue;
        }
        map[key] = d;
      }
    } else {
      for (const d of data) {
        const key = d[col] as string;
        map[key] = d;
      }
    }

    return map;
  },

  toCSV(data: AnyObject[]): string {
    const cols = new Set<string>();
    const escape = (str: any): string => {
      try {
        return `"${str.toString().replace(/"/g, '""')}"`;
      } catch (e) {
        return str;
      }
    };

    const mapList: Record<string, string>[] = [];
    
    for (const d of data) {
      if (d) {
        const tasks: Array<{ obj: AnyObject; pathArr: string[] }> = [{
          obj: d,
          pathArr: []
        }];
        
        const map: Record<string, string> = {};
        
        while (tasks.length > 0) {
          const task = tasks.pop()!;
          const { obj, pathArr } = task;
          
          for (const [key, val] of Object.entries(obj)) {
            const subPathArr = pathArr.concat(key);
            const type = typeof val;
            let val2Set: string | undefined;
            
            if (val && type === 'object') {
              if (Array.isArray(val)) {
                val2Set = escape(val.map(v => {
                  if (typeof v === 'object') {
                    return JSON.stringify(v);
                  } else {
                    return v;
                  }
                }).join(","));
              } else {
                tasks.push({
                  obj: val,
                  pathArr: subPathArr
                });
              }
            } else if (type === 'string') {
              val2Set = escape(val);
            } else {
              if (val !== undefined && val !== null) {
                val2Set = val.toString();
              } else {
                val2Set = val;
              }
            }
            
            if (val2Set !== undefined) {
              const path = subPathArr.join('.');
              map[path] = val2Set;
              cols.add(path);
            }
          }
        }
        
        mapList.push(map);
      }
    }

    const colArr: string[] = [];
    for (const col of cols) {
      colArr.push(col);
    }
    
    const csvArr = [colArr.join(",")];
    
    for (const map of mapList) {
      const arr: string[] = [];
      for (const col of cols) {
        arr.push(map[col] || "");
      }
      csvArr.push(arr.join(','));
    }
    
    return csvArr.join("\n");
  }
};

interface DataUtilModule extends DataUtilFunctions {
  process: (data: AnyObject[], options: ProcessOptions) => any;
}

const dataUtil: DataUtilModule = {
  process(raw: AnyObject[], options: ProcessOptions): any {
    const { agg, query, computed, sort, fields, exclude_fields, group_by, to_map, format } = options;
    let data:AnyObject[] | Record<string, AnyObject | AnyObject[]> = raw;
    
    if (Array.isArray(data)) {
      if (query) {
        data = fns.query(data, query);
      }
      
      if (computed) {
        data = fns.computed(data, computed);
      }
      
      if (sort) {
        data = fns.sort(data, sort);
      }
      
      if (agg) {
        data = fns.agg(data, agg);
      }
      
      if (fields || exclude_fields) {
        data = fns.select(data, fields, exclude_fields);
      }
      
      if (group_by) {
        let field: GroupByField;
        let remove_field = false;
        let pick_one: PickOneOperation | undefined;
        let order: SortOptions | undefined;
        
        if (typeof group_by === 'object') {
          field = group_by.field;
          remove_field = group_by.remove_field || false;
          pick_one = group_by.pick_one;
          order = group_by.order;
        } else {
          field = group_by;
        }
        
        data = fns.groupBy(data, field, remove_field, pick_one, order);
      }else if (to_map) {
        let field: string;
        let multiple = false;
        
        if (typeof to_map === 'object') {
          field = to_map.field;
          multiple = to_map.multiple || false;
        } else {
          field = to_map;
        }
        
        data = fns.toMap(data, field, multiple);
      }
      
        
      if (format === 'csv') {
        if (Array.isArray(data)) {
          return fns.toCSV(data);
        }else{
            const csvMap:{[key:string]:any}={}
            for (const key in data) {
                if(Array.isArray(data[key])){
                    csvMap[key]=fns.toCSV(data[key])
                }else{
                    csvMap[key]=data[key]
                }
            }
            return csvMap
        }
        
        
        
      }
    }

    return data;
  },
  
  ...fns
};

export default dataUtil;

export {
  type AnyObject,
  type MongoQuery,
  type SortOptions,
  type ComputedField,
  type FieldPath,
  type GroupByField,
  type PickOneOperation,
  type ComputedConfig,
  type ComputedFields,
  type GroupByOptions,
  type ToMapOptions,
  type ProcessOptions,
  type DataUtilFunctions,
  type DataUtilModule
};