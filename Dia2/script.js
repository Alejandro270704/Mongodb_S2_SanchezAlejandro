use incautaciones;

db.createCollection("tablaTemporal")
db.createCollection("Departamento")
db.createCollection("Municipio")
db.createCollection("Unidad")
db.createCollection("incautacion")
db.createCollection("incautacionMarihuana")
db.Departamento.createIndex({codDepartamento:1},{unique:true});
db.tablaTemporal.aggregate([
  {
        $group: {
            _id: "$COD_DEPTO",
            nombreDepartamento: { $first: "$DEPARTAMENTO" }
    }
},
  { $project: { codDepartamento: "$_id", nombreDepartamento: 1, _id: 0 } },
  { $merge: { into: "Departamento", on: "codDepartamento", whenMatched: "merge", whenNotMatched: "insert" } }
]);

db.Municipio.createIndex({codMunicipio:1},{unique:true});
db.tablaTemporal.aggregate ([{
  $group:{
    _id:"$COD_MUNI",
    nombreMunicipio:{$first:"MUNICIPIO"},
    Codigo_Departamento:{$first:"$COD_DEPTO"}
  }
},
{ $project: { 
  codMunicipio: "$_id", 
  nombreMunicipio: 1, 
  _id: 0, 
  Codigo_Depto: "$Codigo_Departamento" 
  } 
},
{ $merge: { into: "Municipio", on: "codMunicipio", whenMatched: "merge", whenNotMatched: "insert" } }
])
db.Unidad.createIndex({Unidad:1},{unique:true});
db.tablaTemporal.aggregate([{
    $group:{
        _id:"$UNIDAD"
    }
},
{
    $project:{
        _id:0,
        Unidad:"$_id"
    }
},
{ $merge: { into: "Unidad", on: "Unidad", whenMatched: "merge", whenNotMatched: "insert" } }
])
db.incautacion.createIndex({fecha:1},{unique:true});
db.tablaTemporal.aggregate([
    {
        $group:{
            _id:"$FECHA HECHO",
            Cantidad:{$first:"$CANTIDAD"}
        }
    },
    {
        $project:{
            _id:0,
            fecha:"$_id",
            Cantidad:1
        }
    },
    { $merge: { into: "incautacion", on: "fecha", whenMatched: "merge", whenNotMatched: "insert" } }
])



db.tablaTemporal.aggregate([{
    //municipio
    $lookup:{
        from:"Municipio",
        localField:"COD_MUNI",
        foreignField:"codMunicipio",
        as:"Municipio"

    }
},
  { $unwind: "$Municipio" },
    {$lookup:{
        from:"Departamento",
        localField: "COD_DEPTO",
        foreignField: "codDepartamento",
        as: "Departamento"
    }},
    { $unwind: "$Departamento" },
    {$lookup:{
        from:"Unidad",
        localField:"UNIDAD",
        foreignField:"Unidad",
        as: "Unidad"
    }},
    { $unwind: "$Unidad" },
    {$lookup:{
        from:"incautacion",
        localField:"FECHA HECHO",
        foreignField:"fecha",
        as: "incautacion"
    }},
      { $unwind: "$incautacion" }
    ,{
        $project:{
            _id:0,
            idMunicipio:"$Municipio._id",
            idDepartamento:"$Departamento._id",
            idUnidad:"$Unidad._id",
            idIncautacion:"$incautacion._id"
        }
    },
    {
    $merge: {
      into: "incautacionMarihuana",
      whenNotMatched: "insert"
    }
  }
])   