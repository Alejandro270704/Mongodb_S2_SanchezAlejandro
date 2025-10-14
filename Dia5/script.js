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
      "muni.nombreMunicipio":{$regex: /[A-Za-z]+ [A-Za-z]+$/i}
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
      "muni.nombreMunicipio":{$regex:/[^aeiou]/i}
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