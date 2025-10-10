use incautaciones;
// ¿Cuántos municipios comienzan con "La" y cuál es la cantidad total incautada en ellos?
db.incautacionMarihuana.aggregate([
  {
    $lookup: {
      from: "Municipio",
      localField: "idMunicipio",
      foreignField: "_id",
      as: "Municipio"
    }
  },
  { $unwind: "$Municipio" },
  {
    $lookup: {
      from: "incautacion",
      localField: "idIncautacion",
      foreignField: "_id",
      as: "incautacion"
    }
  },
  { $unwind: "$incautacion" },
  {
    $match: {
      "Municipio.nombreMunicipio": { $regex: /^La/i }

    }
  },
  {
    $group: {
      _id: "$Municipio.nombreMunicipio",
      total: { $sum: "$incautacion.Cantidad" }
    }
  },
  {
    $project: {
      _id: 0,
      nombreMunicipio: "$_id",
      total: 1
    }
  },
  {
    $limit: 5
  }
])

//2.Top 5 departamentos donde los municipios terminan en "al" y la cantidad incautada.


db.incautacionMarihuana.aggregate([
  {
    $lookup: {
      from: "Municipio",
      localField: "idMunicipio",
      foreignField: "_id",
      as: "Municipio"
    }
  },
  { $unwind: "$Municipio" },

  {
    $lookup: {
      from: "incautacion",
      localField: "idIncautacion",
      foreignField: "_id",
      as: "incautacion"
    }
  },
  { $unwind: "$incautacion" },

  {
    $lookup: {
      from: "Departamento",
      localField: "Municipio.Codigo_Depto",
      foreignField: "codDepartamento",
      as: "Departamento"
    }
  },
  { $unwind: "$Departamento" },

  {
    $match: {
      "Municipio.nombreMunicipio": { $regex: /al$/i }
    }
  },

  {
    $group: {
      _id: "$Departamento.nombreDepartamento",
      totalIncautado: { $sum: "$incautacion.Cantidad" }
    }
  },

  {
    $sort: { totalIncautado: -1 }
  },

  {
    $limit: 5
  },

  {
    $project: {
      _id: 0,
      nombreDepartamento: "$_id",
      totalIncautado: 1
    }
  }
]);


//Por cada año, muestra los 3 municipios con más incautaciones, pero únicamente si su nombre contiene la letra "z".
db.incautacionMarihuana.aggregate([
  {
    $lookup: {
      from: "Municipio",
      localField: "idMunicipio",
      foreignField: "_id",
      as: "Municipio"
    }
  },
  { $unwind: "$Municipio" },
  {
    $match: {
      "Municipio.nombreMunicipio": { $regex: /z/i }
    }
  },
  {
    $lookup: {
      from: "incautacion",
      localField: "idIncautacion",
      foreignField: "_id",
      as: "incautacion"
    }
  },
  { $unwind: "$incautacion" },
  {
    $group: {
      _id: {
        anio: { $year: "$incautacion.fecha" },
        municipio: "$Municipio.nombreMunicipio"
      },
      totalIncautaciones: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.anio": 1,
      totalIncautaciones: -1
    }
  },
  {
    $group: {
      _id: "$_id.anio",
      municipios: {
        $push: {
          municipio: "$_id.municipio",
          totalIncautaciones: "$totalIncautaciones"
        }
      }
    }
  },
  {
    $project: {
      anio: "$_id",
      topMunicipios: { $slice: ["$municipios", 3] },
      _id: 0
    }
  },
  {
    $unwind: "$topMunicipios"
  },
  {
    $project: {
      anio: 1,
      municipio: "$topMunicipios.municipio",
      totalIncautaciones: "$topMunicipios.totalIncautaciones"
    }
  }
])
//¿Qué unidad de medida aparece en registros de municipios que empiecen por "Santa"?
db.incautacionMarihuana.aggregate([
  {
    $lookup: {
      from: "Municipio",
      localField: "idMunicipio",
      foreignField: "_id",
      as: "Municipio"
    }
  },
  { $unwind: "$Municipio" },
  {
    $match: {
      "Municipio.nombreMunicipio": { $regex: /^Santa/i }
    }
  },
  {
    $lookup: {
      from: "Unidad",
      localField: "idUnidad",
      foreignField: "_id",
      as: "Unidad"
    }
  },
  { $unwind: "$Unidad" },
  {
    $project: {
      _id: 0,
      Municipio: "$Municipio.nombreMunicipio",
      Unidad: "$Unidad.Unidad"
    }
  }
])
//¿Cuál es la cantidad promedio de incautaciones en los municipios cuyo nombre contiene "Valle"?
db.incautacionMarihuana.aggregate([
  {
    $lookup: {
      from: "Municipio",
      localField: "idMunicipio",
      foreignField: "_id",
      as: "Municipio"
    }
  },
  { $unwind: "$Municipio" },
  {
    $match:{
      "Municipio.nombreMunicipio":{$regex:/Valle/i}
    }
  },
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"incautacion"
    }
  },
  { $unwind: "$incautacion" },
  {
    $group:{
      _id:"$Municipio.nombreMunicipio",
      promedioCantidad:{$avg: "$incautacion.Cantidad"}
    }
  },
  {
    $project:{
       _id: 0,
      Municipio: "$_id",
      promedioCantidad: 1
    }
  },
  { $sort: { promedioCantidad: -1 } }
]);


//¿Cuántos registros hay en municipios cuyo nombre contenga exactamente 7 letras?
db.incautacionMarihuana.aggregate([
  {
    $lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"municipio"}
  },
  {$unwind:"$municipio"},
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"incautacion"
    }
  },
  {
    $match:{
      "municipio.nombreMunicipio":{$regex:/^[A-Za-zÁÉÍÓÚáéíóúÑñ]{7}$/}
    }
  },
  {
    $group:{
      _id:"$municipio.nombreMunicipio",
      totalIncautaciones: { $sum: 1 }
    }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id",
      totalIncautaciones:1

    }
  }
])

//¿Cuáles son los 10 municipios con mayor cantidad incautada en 2020?
db.incautacionMarihuana.aggregate([
  
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  {$unwind: "$inca"},
  {
    $addFields:{
      anio:{$year:"$inca.fecha"}
    }
  },
  {
    $match:{
      anio:2020
    }
  },
  {
    $group:{
      _id:"$muni.nombreMunicipio",
      cantidad:{$sum:"$inca.Cantidad"}
      
    }
  },
  {
    $sort:{cantidad:-1}
  },
  {
    $limit:10
  },
  {
    $project:{
      _id:0,
      municipio:"$_id",
      cantidad:1
    }
  }

]);
//Por cada departamento, muestra el municipio con más cantidad incautada.
db.incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  {$unwind: "$inca"},
  {
    $lookup: {
      from: "Departamento",
      localField: "Municipio.Codigo_Depto",
      foreignField: "codDepartamento",
      as: "Departamento"
    }
  },
  { $unwind: "$Departamento" },
  {
    $group:{
      _id:{depar:"$Departamento.nombreDepartamento",
        muni:"$muni.nombreMunicipio"
      },
      totalIncautaciones:{$sum:"$incautacion.Cantidad"}
    }
  },
  {
    $sort:{totalIncautaciones:-1}
  },
  
    {
    $group: {
      _id: "$_id.departamento",
      municipio: { $first: "$_id.municipio" },
      cantidadIncautacion: { $first: "$totalIncautaciones" }
    }
  },
  {
    $project:{
      _id:0,
      departamento:"$_id",
      municipio:"$municipio",
      cantidadIncautacion:"cantidadIncautacion"
    }
  }
])
//Muestra la evolución mensual de incautaciones en Antioquia.
//falta por hacer,investigar sintaxis de este problema.

//¿Cuáles son los 5 años con menor cantidad incautada en todo el país?
db.incautacion.aggregate([
  {
    $addFields:{
      anio:{$year:"$fecha"}
    }
  },
  {
    $group:{
      _id:"$anio",
      total:{$sum:"$Cantidad"}
    }
  },
  {
    $sort:{total:-1}
  },
  {
    $limit:5
  },
  {
    $project:{
      _id:0,
      anio:"$_id",
      cantidad:"$total"
    }
  }
])
//Muestra la cantidad total incautada por unidad de medida, ordenada de mayor a menor.
db.incautacionMarihuana.aggregate([
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  { $unwind: "$inca" },
  {
    $lookup:{
      from:"Unidad",
      localField:"idUnidad",
      foreignField:"_id",
      as:"unidad"
    }
  },
  { $unwind: "$unidad" },
  {
    $group:{
      _id:"$unidad.Unidad",
      unidad: { $sum: "$inca.Cantidad" }
    }
  },
  {
    $sort:{unidad:-1}
  },
  {
    $project:{
      _id:0,
      cantidad:"$_id",
      unidad:"$unidad"
    }
  }
])
//Identifica los municipios que nunca superaron 1 kilogramo de incautación en todos sus registros.
db.incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  { $unwind: "$inca" },
  {
    $group:{
      _id:"$muni.nombreMunicipio",
      cantidad: { $max: "$inca.Cantidad" }
    }
  },
  {
    $match:{
      cantidad:{ $lte: 1 }
    }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id",
      cantidad:"$cantidad"
    }
  }

])
//Encuentra los municipios cuyo nombre empieza por "Puerto" y determina el total incautado en cada año.
db.incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  { $unwind: "$inca" },
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/Puerto/i}
    }
  },
  {
  $group: {
    _id: {
      municipio: "$muni.nombreMunicipio",
      anio: { $year: "$inca.fecha" }
    },
    total: { $sum: "$inca.Cantidad" }
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id.municipio",
      anio:"$_id.anio",
      total:"$total"
    }
  },
  {
    $sort: { municipio: 1, anio: 1 }
  }


])
//¿Cuál es el mes con más incautaciones en toda la historia para municipios que terminen en "ito" o "ita"?
db.incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  { $unwind: "$inca" },
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/(ito|ita)$/i}
    }
  },
  {
    $group:{
      _id:{
        mes:{$month:"$inca.fecha"},
        municipio:"$muni.nombreMunicipio"
      },
      cantidad:{$sum:"$inca.Cantidad"}
    }
  },
  {
    $sort:{cantidad:-1}
  },
  {
    $project:{
      _id:0,
      mes:"$_id.mes",
      municipio:"$_id.municipio",
      cantidad:1

    }
  }

])
//Construye un ranking de los 5 departamentos con mayor variación de incautaciones entre el primer y el último año registrado.

db.incautacionMarihuana.aggregate([
  {
    $lookup: {
      from: "Municipio",
      localField: "idMunicipio",
      foreignField: "_id",
      as: "muni"
    }
  },
  { $unwind: "$muni" },
  {
    $lookup: {
      from: "incautacion",
      localField: "idIncautacion",
      foreignField: "_id",
      as: "inca"
    }
  },
  { $unwind: "$inca" },
  {
    $lookup: {
      from: "Departamento",
      localField: "muni.Codigo_Depto",
      foreignField: "codDepartamento",
      as: "depto"
    }
  },
  { $unwind: "$depto" },

  
])