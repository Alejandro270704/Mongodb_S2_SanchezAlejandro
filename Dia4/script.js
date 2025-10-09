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
      "Municipio.nombreMunicipio": { $regex: /z/i }
    }
  },
  {
    $addFields: {
      anio: { $year: "$incautacion.fecha" }
    }
  },
  {
    $group: {
      _id: { anio: "$anio", municipio: "$Municipio.nombreMunicipio" },
      totalIncautaciones: { $sum: 1 }
    }
  },
  {
    $limit: 3
  },
   {
    $project: {
      _id: 0,
      anio: "$_id.anio",
      municipio: "$_id.municipio",
      totalIncautaciones: 1
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
  {
    $limit:5
  }
]);


