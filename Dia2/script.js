use incautaciones;

db.createCollection("tablaTemporal")
db.createCollection("Departamento")
db.createCollection("Municipio")
db.createCollection("Unidad")
db.createCollection("incautacion")
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
    nombreMunicipio:{$first:"$MUNICIPIO"},
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
    $project: {
      fechaOriginal: "$FECHA HECHO",
      Cantidad: "$CANTIDAD"
    }
  },
  {
    $addFields: {
      fecha: {
        $dateFromString: {
          dateString: "$fechaOriginal",
          format: "%d/%m/%Y",
          timezone: "UTC"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      fecha: 1,
      Cantidad: 1
    }
  },
  {
    $merge: {
      into: "incautacion",
      on: "fecha",
      whenMatched: "merge",
      whenNotMatched: "insert"
    }
  }
]);


db.createCollection("incautacionMarihuana");
db.tablaTemporal.aggregate([
  // Convertir fecha
  {
    $addFields: {
      fechaConvertida: {
        $dateFromString: {
          dateString: "$FECHA HECHO",
          format: "%d/%m/%Y",
          timezone: "UTC"
        }
      }
    }
  },
  // Lookup con Municipio
  {
    $lookup: {
      from: "Municipio",
      localField: "COD_MUNI",
      foreignField: "codMunicipio",
      as: "Municipio"
    }
  },
  { $unwind: "$Municipio" },
  // Lookup con Unidad
  {
    $lookup: {
      from: "Unidad",
      localField: "UNIDAD",
      foreignField: "Unidad",
      as: "Unidad"
    }
  },
  { $unwind: "$Unidad" },
  // Lookup con Incautacion (usando la fecha convertida correctamente)
  {
    $lookup: {
      from: "incautacion",
      localField: "fechaConvertida",
      foreignField: "fecha",
      as: "incautacion"
    }
  },
  { $unwind: "$incautacion" },
  // Seleccionar solo los IDs
  {
    $project: {
      _id: 0,
      idMunicipio: "$Municipio._id",
      idUnidad: "$Unidad._id",
      idIncautacion: "$incautacion._id"
    }
  },
  // Insertar en incautacionMarihuana
  {
    $merge: {
      into: "incautacionMarihuana",
      whenNotMatched: "insert"
    }
  }
]);