import dataUtil from "./dist/data-util.js";
const data=[{
    id:0,
    group:'A',
    name:'ssss'
},{
    id:1,
    group:'B',
    name:'gggg'
},{
    id:2,
    group:'A',
    name:'ssss'
},{
    id:3,
    group:'B',
    name:'rdssd'
},{
    id:4,
    group:'A',
    name:'kyuky'}]

    console.log(dataUtil.groupBy(data,'group'))