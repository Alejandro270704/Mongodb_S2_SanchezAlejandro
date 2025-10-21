use institucion;

db.createCollection("tablaCruda");

db.createCollection('Establecimiento', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: [
                'nombreestablecimiento', 'correoElectronico', 'direccion', 'grado',
                'idAnio', 'idJornada', 'idTipoEspecialidad', 'idZonaEstablecimiento',
                'nivel', 'nombreRector', 'numeroSede', 'telefono'
              ],
            properties: {
                nombreestablecimiento: { bsonType: 'string' },
                correoElectronico: { bsonType: 'string' },
                direccion: { bsonType: 'string' },
                grado: { bsonType: 'string' },
                idAnio: { bsonType: 'objectId' },
                idJornada: { bsonType: 'objectId' },
                idTipoEspecialidad: { bsonType: 'objectId' },
                idZonaEstablecimiento: { bsonType: 'objectId' },
                nivel: { bsonType: 'string' },
                nombreRector: { bsonType: 'string' },
                numeroSede: { bsonType: 'int' },
                telefono: { bsonType: 'string' }
              }
        }
    }
})

// 
db.createCollection('Tiempo', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: [
                'anio'],
            properties: {
                anio: {
                    bsonType: 'int'
                }
            }
        }
    }
})
//
db.createCollection('Jornada', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['tipo'],
            properties: {
                tipo: {
                    bsonType: 'string'
                }
            }
        }
    }
})
//
db.createCollection('Estado', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['_id','estado'],
            properties: {
                _id: {
                    bsonType: 'int'
                },
                estado: {
                    bsonType: 'string'
                }
            }
        }
    }
})
//
db.createCollection('ZonaEstablecimiento', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['zona'],
            properties: {
                zona: {
                    bsonType: 'string'
                }
            }
        }
    }
})
//
db.createCollection('TipoEspecialidad', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['especialidad'],
            properties: {
                especialidad: {
                    bsonType: 'string'
                }
            }
        }
    }
})

//
db.tablaCruda.updateMany(
    {},
    [
      {
        $set: {
          "a��o": {
            $toInt: {
              $replaceAll: {
                input: "$a��o",
                find: ",",
                replacement: ""
              }
            }
          },
          "grados": {
            $toString: "$grados"
          },
          "telefono":{
            $toString: "$telefono"
          }
        }
      }
    ]
  )
//insersiones
db.tablaCruda.aggregate([
    {
        $project: {
            tipo: "$jornadas"

        }
    },
    {
        $group: {
            _id: "$tipo"
        }
    },
    {
        $project: {
            _id: 0,
            tipo: "$_id"
        }
    },
    { $merge: { into: "Jornada", whenMatched: "keepExisting", whenNotMatched: "insert" } }
])
// 
db.tablaCruda.aggregate([
    {
        $project: {
            especialidad: "$especialidad"

        }
    },
    {
        $group: {
            _id: "$especialidad"
        }
    },
    {
        $project: {
            _id: 0,
            especialidad: "$_id"
        }
    },
    { $merge: { into: "TipoEspecialidad", whenMatched: "keepExisting", whenNotMatched: "insert" } }
])
//
db.tablaCruda.aggregate([
    {
        $project: {
            zona: "$zona"

        }
    },
    {
        $group: {
            _id: "$zona"
        }
    },
    {
        $project: {
            _id: 0,
            zona: "$_id"
        }
    },
    { $merge: { into: "ZonaEstablecimiento", whenMatched: "keepExisting", whenNotMatched: "insert" } }
])
//
db.tablaCruda.aggregate([
    {
        $project: {
            anio: "$a��o",

        }
    },
    {
        $group: {
            _id: "$anio"
        }
    },
    {
        $project: {
            _id: 0,
            anio: "$_id"
        }
    },
    { $merge: { into: "Tiempo", whenMatched: "keepExisting", whenNotMatched: "insert" } }
])
db.Estado.insertMany([
    {
        _id: 1,
        estado: "activo"
    },
    {
        _id: 2,
        estado: "inactivo"
    }
])
db.tablaCruda.aggregate([
    {
        $lookup: {
            from: "Jornada",
            localField: "jornadas",
            foreignField: "tipo",
            as: "Jornada"
        }
    },
    { $unwind: "$Jornada" },

    {
        $lookup: {
            from: "ZonaEstablecimiento",
            localField: "zona",
            foreignField: "zona",
            as: "ZonaEstablecimiento"
        }
    },
    { $unwind: "$ZonaEstablecimiento" },

    {
        $lookup: {
            from: "TipoEspecialidad",
            localField: "especialidad",
            foreignField: "especialidad",
            as: "TipoEspecialidad"
        }
    },
    { $unwind: "$TipoEspecialidad" },

    {
        $lookup: {
            from: "Tiempo",
            localField: "a��o",
            foreignField: "anio",
            as: "Tiempo"
        }
    },
    { $unwind: "$Tiempo" },

    {
        $project: {
            
            nombreestablecimiento: "$nombreestablecimiento",
            direccion: "$direccion",
            nombreRector: "$nombre_Rector",
            nivel: "$niveles",
            telefono: "$telefono",
            correoElectronico: "$correo_Electronico",
            numeroSede: "$numero_de_Sedes",
            grado: "$grados",

            idJornada: "$Jornada._id",
            idZonaEstablecimiento: "$ZonaEstablecimiento._id",
            idTipoEspecialidad: "$TipoEspecialidad._id",
            idAnio: "$Tiempo._id"

        }
    },
    {
        $group: {
            _id: "$nombreestablecimiento", 
            direccion: { $first: "$direccion" },
            nombreRector: { $first: "$nombreRector" },
            nivel: { $first: "$nivel" },
            telefono: { $first: "$telefono" },
            correoElectronico: { $first: "$correoElectronico" },
            numeroSede: { $first: "$numeroSede" },
            grado: { $first: "$grado" },
            idJornada: { $first: "$idJornada" },
            idZonaEstablecimiento: { $first: "$idZonaEstablecimiento" },
            idTipoEspecialidad: { $first: "$idTipoEspecialidad" },
            idAnio: { $first: "$idAnio" },
            
        }
    },
    {
        $project: {
            _id: 0, 
            nombreestablecimiento: "$_id",
            direccion: 1,
            nombreRector: 1,
            nivel: 1,
            telefono: 1,
            correoElectronico: 1,
            numeroSede: 1,
            grado: 1,
            idJornada: 1,
            idZonaEstablecimiento: 1,
            idTipoEspecialidad: 1,
            idAnio: 1
          }
    },
    { $merge: { into: "Establecimiento", whenMatched: "keepExisting", whenNotMatched: "insert" } }
])
db.Establecimiento.updateMany(
  {},                       
  { $set: { 
    idEstado: 1 ,
    sector:444
  } } 
);
// validacion