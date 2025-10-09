use incautaciones;
// ¿Cuántos municipios comienzan con "La" y cuál es la cantidad total incautada en ellos?
db.incautacionMarihuana.aggregate([
    {
        $lookup:{
            from:"Municipio",
            localField:"idMunicipio",
            foreignField:"_id",
            as: "Municipio"
        }
    },
    { $unwind: "$Municipio" },
    {
        $lookup:{
            from:"incautacion",
            localField:"idIncautacion",
            foreignField:"_id",
            as: "incautacion"
        }
    },
    { $unwind: "$incautacion" },
    {
        $match:{
            "Municipio.nombreMunicipio":{$regex: /^La/i }

        }  
    },
    {
        $group:{
            _id:"$Municipio.nombreMunicipio",
            total:{$sum:"$incautacion.Cantidad"}
        }
    },
    {
        $project:{
            _id:0,
            nombreMunicipio:"$_id",
            total:1
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
        $lookup:{
            from:"Municipio",
            localField:"idMunicipio",
            foreignField:"_id",
            as:"Municipio"
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
    $match:{
        "Municipio.nombreMunicipio":{$regex:/z/i}
    }
  },
  {
    $addFields:{
        anio:{$year:"$incautacion.fecha"}
    }
  },
   {
    $group: {
      _id: { anio: "$anio", municipio: "$Municipio.nombreMunicipio" },
      totalIncautaciones: { $sum: 1 }
    }
  },
   

])