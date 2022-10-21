const mingo=require('mingo')
const math=require('mathjs')
const ObjUtil=require('@nervmouse/object-util')
 
const fns={
  query(data,opts){
    const MongoQ=new mingo.Query(opts)
    const cursor=MongoQ.find(data)
    return cursor.all()
  },
  agg(data,piplines){
    return mingo.aggregate(data,piplines)
  },
  sort(data,opts){
    const MongoQ=new mingo.Query({})
    const cursor=MongoQ.find(data)
    cursor.sort(opts)
    return cursor.all()
  },
  computed(data,computed){
    const cmpArr=[]
    const env={}
    let scope_name
    let cfg_inplace=true
    for(let prop in computed){
      if(prop === '$config'){
        let {require:packages,data_scope,inplace}=computed['$config']
        if (inplace!==undefined){
          cfg_inplace=inplace
        }
        scope_name=data_scope
        if (packages){
          for(let pkg of packages){
            env[pkg]=require(pkg)
          }
        }
        
      }else{
        const cmp=math.compile(computed[prop])
        cmp.prop=prop
        cmpArr.push(cmp)
      }
      
    }
    const new_data=[]
    for(let d of data){
      let new_d={}
      for(let cmp of cmpArr){
        
        try{
          let scope
          if (scope_name){
            scope={...env,[scope_name]:d}
          }else{
            scope={...env,...d}
          }
          const val=cmp.evaluate(scope)
          if (cfg_inplace){
            ObjUtil.objPathSet(d,cmp.prop,val,true)
            new_data.push(d)
          }else{
            ObjUtil.objPathSet(new_d,cmp.prop,val,true)
            new_data.push(new_d)
          }
          
          
        }catch(e){
          console.log(e)
        }
      }
    }
    return new_data
  },
  select(data,fields,exclude_fields){
    let new_data=[]
			if (!fields && !exclude_fields){
				return data
			}
			for(let rd of data){
				let d={}
				if (fields){
					for(let field of fields){
						ObjUtil.objPathSet(
							d,
							field,
							ObjUtil.objPath(rd,field),
							true
						)	
					}
				}else{
					Object.assign(d,rd)
				}
				if (exclude_fields){
					for(let field of exclude_fields){
						ObjUtil.objPathDelete(d,field)
					}
				}
				new_data.push(d)
			}
		return new_data
  },
  groupBy(data,field,remove_field,pick_one,order){
    
    const map={}
		for(const d of data){
				const key=d[field]
				if(!map[key]){
					map[key]=[]
				}
				if (remove_field){
					delete d[field]
				}
				map[key].push(d)

    }
    if (order) {
      for (let key in map) {
        const sortOrder = {
          asc: [],
          desc: []
        }
        for (let orderKey in order) {
          if (order[orderKey] === 1) {
            sortOrder.asc.push(orderKey)
          } else if (order[orderKey] === -1) {
            sortOrder.desc.push(orderKey)
          }
        }
        // desc first
        for (let descKey of sortOrder.desc) {
          map[key] = map[key].sort((a, b) => {
            return order[descKey] * (new Date(a[descKey]) - new Date(b[descKey]))
          })
        }
        for (let ascKey of sortOrder.asc) {
          map[key] = map[key].sort((a, b) => {
            return order[ascKey] * (new Date(a[ascKey]) - new Date(b[ascKey]))
          })
        }
      }
    }
    if (pick_one){
      
      for(let key of Object.keys(map)) {
        const arr=map[key]
        map[key]=fns.pickOne(arr,pick_one)
      }
    }
    
		return map
  },
  pickOne(data,oper){
    const operFn={
      first(arr){
        return arr[0]
      },
      last(arr){
        if(arr.length>0){
          return arr[arr.length-1]
        }else{
          return undefined
        }
      }
    }
    const operType=typeof oper
    if (operType==='string'){
      return operFn[oper](data)
    }else if (operType==='number'){
      return data[oper]
    }
  },
  toMap(data,col){
    const map={}
    for(const d of data){
      const key=d[col]
      if(map[key]){
        if(!Array.isArray(map[key])) map[key] = [map[key]]
        map[key].push(d)
        continue
      }
      map[key]=d
    }
    return map
  },
  toCSV(data){
    const cols=new Set()
    const csvMap={}
    const escape=((str)=>{
      try{
        return `"${str.replace(/\"/g, '""')}"`
      }catch(e){
        return str
      }
      
    })
    const mapList=[]
    for (let d of data){
      if (d){
        const tasks=[{
          obj:d,
          pathArr:[]
        }]
        const map={}
        while (tasks.length>0){
          const task=tasks.pop()
          let {obj,pathArr}=task
          for(let [key,val] of Object.entries(obj)){
            const subPathArr=pathArr.concat(key)
            
            const type=typeof val
            let val2Set
            if (val && type==='object'){
              if (Array.isArray(val)){
                val2Set= escape(val.map(v=>{
                  if (typeof v==='object'){
                    return JSON.stringify(v)
                  }else{
                    return v
                  }
                }).join(","))
                //console.log(val2Set)
              }else{
                tasks.push({
                  obj:val,
                  pathArr:subPathArr
                })
              }
            }else if(type==='string'){
              val2Set= escape(val)
            }else{
              if (val!==undefined && val!==null){
                val2Set= val.toString()
              }else{
                val2Set= val
              }
              
            }
            if (val2Set!==undefined){
              const path=subPathArr.join('.')
              map[path]=val2Set
              cols.add(path)
            }
    
          }
        }
        mapList.push(map)
      }
      
      
    }
    
    const colArr=[]
    for(let col of cols){
      colArr.push(col)
    }
    const csvArr=[colArr.join(",")]
    for(let map of mapList){
      const arr=[]
      for(let col of cols){
        arr.push(map[col] || "")
      }
      csvArr.push(arr.join(','))
    }
    return csvArr.join("\n")
  }
}
module.exports ={
  process(data,options){
    let {agg,query,computed,sort,fields,exclude_fields,group_by,to_map,format}=options
    if (Array.isArray(data)){
      if (query){
        data=fns.query(data,query)
      }
      if (computed){
        data=fns.computed(data,computed)
      }
      if (sort){
        data= fns.sort(data,sort)
      }
      if (agg){
        data=fns.agg(data,agg)
      }
      if (fields || exclude_fields){
        data=fns.select(data,fields,exclude_fields)
      }
      if (group_by){
        let field=group_by
        let remove_field=false
        let pick_one=null
        if (typeof group_by ==='object'){
          field=group_by.field
          remove_field=group_by.remove_field
          pick_one=group_by.pick_one
        }
        
        data=fns.groupBy(data,field,remove_field,pick_one)
        
      }
      if (to_map){
        let field=to_map
        if (typeof to_map ==='object'){
          field=to_map.field
        }
        data=fns.toMap(data,field)
      }
      if (format==='csv'){
        data=fns.toCSV(data)
      }
      
    }

    return data
  },
  ...fns
}