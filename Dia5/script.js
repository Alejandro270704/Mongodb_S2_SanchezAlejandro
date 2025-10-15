use incautaciones;
//Por favor realizar los siguientes ejercicios , donde se apliquen transacciones :
session = db.getMongo().startSession();

session.startTransaction();
//1. Encuentra todos los municipios que empiezan por “San”.
session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/^San/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//2. Lista los municipios que terminan en “ito”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
    {
        $lookup:{
            from:municipio,
            localField:"idMunicipio",
            foreignField:"_id",
            as: "muni"
        }
    },
    {
        $unwind:"$muni"
    },
    {
        $match:{$regex:/ ito$/i}
    },
    {
        $group:{
            _id:"$muni.nombreMunicipio"
        }
    },
    {
        $project:{
            _id:0,
            municipio:"$_id"
        }
    }
])
session.commitTransaction();
session.endSession();
//3. Busca los municipios cuyo nombre contenga la palabra “Valle”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/Valle/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//4. Devuelve los municipios cuyo nombre empiece por vocal.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/^[aeiou]/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//5. Filtra los municipios que terminen en “al” o “el”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/(al|el)$/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//6. Encuentra los municipios cuyo nombre contenga dos vocales seguidas.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/[aeiou]{2}/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//7. Obtén todos los municipios con nombres que contengan la letra “z”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/z/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//8. Lista los municipios que empiecen con “Santa” y tengan cualquier cosa después.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/^santa/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//9. Encuentra municipios cuyo nombre tenga exactamente 6 letras.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex: /[A-Za-z]{6}/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//10. Filtra los municipios cuyo nombre tenga 2 palabras
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex: /^[A-Za-z]+ [A-Za-z]+$/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//11. Encuentra municipios cuyos nombres terminen en “ito” o “ita”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/(ito|ita)$/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//12. Lista los municipios que contengan la sílaba “gua” en cualquier posición.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/gua/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//13. Devuelve los municipios que empiecen por “Puerto” y terminen en “o”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/^puerto.*o$/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//14. Encuentra municipios con nombres que tengan más de 10 caracteres.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/.{11}/}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }

])
session.commitTransaction();
session.endSession();
//15. Busca municipios que no contengan vocales.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
  {$lookup:{
    from:"Municipio",
    localField:"idMunicipio",
    foreignField:"_id",
    as:"muni"
  }},
  {$unwind: "$muni"},
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/^[^aeiou]+$/i}
    }
  },
  {
  $group: {
    _id:  "$muni.nombreMunicipio"
  }
  },
  {
    $project:{
      _id:0,
      municipio:"$_id"
    }
  }


])
session.commitTransaction();
session.endSession();
//16. Muestra la cantidad total incautada en municipios que empiezan con “La”.
session = db.getMongo().startSession();
session.startTransaction();

session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
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
  }
])


session.commitTransaction();
session.endSession();
//17. Calcula el total de incautaciones en municipios cuyo nombre termine en “co”.
session = db.getMongo().startSession();
session.startTransaction();
session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
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
      "Municipio.nombreMunicipio": { $regex: /co$/i }

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
  }
])
session.commitTransaction();
session.endSession();
//18. Obtén el top 5 de municipios con más incautaciones cuyo nombre contenga la letra “y”.
session = db.getMongo().startSession();
session.startTransaction();
session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
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
      "Municipio.nombreMunicipio": { $regex: /y$/i }

    }
  },
  {
    $group: {
      _id: "$Municipio.nombreMunicipio",
      total: { $sum: "$incautacion.Cantidad" }
    }
  },
  {$sort:{total:-1}},
  {
    $project: {
      _id: 0,
      nombreMunicipio: "$_id",
      total: 1
    }
  },
  {$limit:5}
])
session.commitTransaction();
session.endSession();
//19. Encuentra los municipios que empiecen por “San” y agrupa la cantidad incautada por año.
session = db.getMongo().startSession();
session.startTransaction();
session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
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
    $addFields:{
      anio:{$year:"$incautacion.fecha"}
    }
  },
  {
    $match: {
      "Municipio.nombreMunicipio": { $regex: /^san/i }

    }
  },
  {
    $group: {
      _id:{municipio:"$Municipio.nombreMunicipio",anio:"$anio"}, 
      total: { $sum: "$incautacion.Cantidad" }
    }
  },
  {
    $project: {
      _id: 0,
      nombreMunicipio: "$_id.municipio",
      anio:"$_id.anio",
      total: 1
    }
  }
])
session.commitTransaction();
session.endSession();
//20. Lista los departamentos que tengan al menos un municipio cuyo nombre termine en “ito” o “ita”, y muestra la cantidad total incautada en ellos.
session = db.getMongo().startSession();
session.startTransaction();
session.getDatabase("incautaciones").incautacionMarihuana.aggregate([
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
    $lookup: {
      from: "Departamento",
      localField: "muni.Codigo_Depto",
      foreignField: "codDepartamento",
      as: "Departamento"
    }
  },
  { $unwind: "$Departamento" },
  {
    $match:{
      "muni.nombreMunicipio":{$regex:/(ito|ita)$/i}
    }
  },
  {
    $group:{
      _id:{depar:"$Departamento.nombreDepartamento",
        muni:"$muni.nombreMunicipio"
      },
      cantidad:{$sum:"$inca.Cantidad"},
    }
  },
  {
    $sort:{cantidad:-1}
  },
  {
    $project:{
      _id:0,
      departamento:"$_id.depar",
      municipio:"$_id.muni",
      cantidad:1

    }
  }

])

session.commitTransaction();
session.endSession();