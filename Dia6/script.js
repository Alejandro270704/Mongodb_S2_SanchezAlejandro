use institucion;

db.createCollection("tablaCruda");

db.createCollection("Establecimiento");
db.createCollection("Tiempo");
db.createCollection("Jornada");
db.createCollection("Estado");
db.createCollection("ZonaEstablecimiento");
db.createCollection("TipoEspecialidad");


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
            anio: "$a��o"

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
            _id: "$nombrestablecimiento"
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