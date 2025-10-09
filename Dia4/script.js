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


//¿Cuáles son los 10 municipios con mayor cantidad incautada en 2020?
db.incautacionMarihuana.aggregate([
  {
    $lookup:{
      from:"incautacion",
      localField:"idIncautacion",
      foreignField:"_id",
      as:"inca"
    }
  },
  {$unwind: "inca"},
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},

])