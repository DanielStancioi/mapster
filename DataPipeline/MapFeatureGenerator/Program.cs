using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using CommandLine;
using Mapster.Common;
using Mapster.Common.MemoryMappedTypes;
using OSMDataParser;
using OSMDataParser.Elements;

namespace MapFeatureGenerator;

public static class Program
{
    private static MapData LoadOsmFile(ReadOnlySpan<char> osmFilePath)
    {
        var nodes = new ConcurrentDictionary<long, AbstractNode>();
        var ways = new ConcurrentBag<Way>();

        Parallel.ForEach(new PBFFile(osmFilePath), (blob, _) =>
        {
            switch (blob.Type)
            {
                case BlobType.Primitive:
                    {
                        var primitiveBlock = blob.ToPrimitiveBlock();
                        foreach (var primitiveGroup in primitiveBlock)
                            switch (primitiveGroup.ContainedType)
                            {
                                case PrimitiveGroup.ElementType.Node:
                                    foreach (var node in primitiveGroup) nodes[node.Id] = (AbstractNode)node;
                                    break;

                                case PrimitiveGroup.ElementType.Way:
                                    foreach (var way in primitiveGroup) ways.Add((Way)way);
                                    break;
                            }

                        break;
                    }
            }
        });

        var tiles = new Dictionary<int, List<long>>();
        foreach (var (id, node) in nodes)
        {
            var tileId = TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude));
            if (tiles.TryGetValue(tileId, out var nodeIds))
            {
                nodeIds.Add(id);
            }
            else
            {
                tiles[tileId] = new List<long>
                {
                    id
                };
            }
        }

        return new MapData
        {
            Nodes = nodes.ToImmutableDictionary(),
            Tiles = tiles.ToImmutableDictionary(),
            Ways = ways.ToImmutableArray()
        };
    }

    private static void CreateMapDataFile(ref MapData mapData, string filePath)
    {
        var usedNodes = new HashSet<long>();

        long featureIdCounter = -1;
        var featureIds = new List<long>();
        // var geometryTypes = new List<GeometryType>();
        // var coordinates = new List<(long id, (int offset, List<Coordinate> coordinates) values)>();

        var labels = new List<int>();
        // var propKeys = new List<(long id, (int offset, IEnumerable<string> keys) values)>();
        // var propValues = new List<(long id, (int offset, IEnumerable<string> values) values)>();

        using var fileWriter = new BinaryWriter(File.OpenWrite(filePath));
        var offsets = new Dictionary<int, long>(mapData.Tiles.Count);

        // Write FileHeader
        fileWriter.Write((long)1); // FileHeader: Version
        fileWriter.Write(mapData.Tiles.Count); // FileHeader: TileCount

        // Write TileHeaderEntry
        foreach (var tile in mapData.Tiles)
        {
            fileWriter.Write(tile.Key); // TileHeaderEntry: ID
            fileWriter.Write((long)0); // TileHeaderEntry: OffsetInBytes
        }

        foreach (var (tileId, _) in mapData.Tiles)
        {
            // FIXME: Not thread safe
            usedNodes.Clear();

            // FIXME: Not thread safe
            featureIds.Clear();
            labels.Clear();

            var totalCoordinateCount = 0;
            var totalPropertyCount = 0;

            var featuresData = new Dictionary<long, FeatureData>();

            foreach (var way in mapData.Ways)
            {
                var featureId = Interlocked.Increment(ref featureIdCounter);

                var featureData = new FeatureData
                {
                    Id = featureId,
                    Coordinates = (totalCoordinateCount, new List<Coordinate>()),
                    PropertyKeys = (totalPropertyCount, new List<MapFeatureData.EnumKeysProp>(way.Tags.Count)),
                    PropertyValues = (totalPropertyCount, new List<MapFeatureData.StructValuesProp>(way.Tags.Count))
                };

                var geometryType = GeometryType.Polyline;
                featureIds.Add(way.Id);

                labels.Add(-1);

                var added = true;

                //MapFeatureData.EnumKeysProp enumKeys;
                //MapFeatureData.StructValuesProp.EnumValuesProp enumValues;
                
                foreach (var tag in way.Tags)
                {
                    added = true;
                    if (tag.Key.StartsWith("natural")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.natural);
                    }
                    else if (tag.Key.StartsWith("place")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.place);
                    }
                    else if (tag.Key.StartsWith("boundary")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.boundary);
                    }
                    else if (tag.Key.StartsWith("admin_level")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.admin_level);
                    }
                    else if (tag.Key.StartsWith("highway")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.highway);
                    }
                    else if (tag.Key.StartsWith("water")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.water);
                    }
                    else if (tag.Key.StartsWith("railway")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.railway);
                    }
                    else if (tag.Key.StartsWith("landuse")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.landuse);
                    }
                    else if (tag.Key.StartsWith("name")) { 
                        labels[^1] = totalPropertyCount * 2 + featureData.PropertyKeys.keys.Count * 2 + 1;
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.name);
                        MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.none, tag.Value);
                        featureData.PropertyValues.values.Add(structValuesProp);
                        added = false;
                        
                    }
                    else if (tag.Key.StartsWith("leisure")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.leisure);
                    }
                    else if (tag.Key.StartsWith("amenity")) { 
                        featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.amenity);
                    }
                    else { added = false;}

                    if (added) { 
                        if (tag.Value.StartsWith("fell") || tag.Value.StartsWith("grassland") || tag.Value.StartsWith("heath") || tag.Value.StartsWith("moor")
                            || tag.Value.StartsWith("scrub") || tag.Value.StartsWith("wetland")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.fell, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("wood") || tag.Value.StartsWith("tree_row")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.wood, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("bare_rock") || tag.Value.StartsWith("rock") || tag.Value.StartsWith("scree")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.rock, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("beach") || tag.Value.StartsWith("sand") ) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.sand, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("water")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.water, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("city") || tag.Value.StartsWith("town") || tag.Value.StartsWith("locality") 
                            || tag.Value.StartsWith("hamlet")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.city, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("motorway") || tag.Value.StartsWith("trunk") || tag.Value.StartsWith("primary") 
                            || tag.Value.StartsWith("secondary") || tag.Value.StartsWith("tertiary") || tag.Value.StartsWith("road")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.road, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if ( tag.Value.StartsWith("administrative")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.administrative, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("forest") || tag.Value.StartsWith("orchad")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.forest, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("residential") || tag.Value.StartsWith("cemetery") || tag.Value.StartsWith("industrial") 
                            || tag.Value.StartsWith("commercial") || tag.Value.StartsWith("square") || tag.Value.StartsWith("construction")
                            || tag.Value.StartsWith("military")|| tag.Value.StartsWith("quarry")|| tag.Value.StartsWith("brownfield")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.construction, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("farm") || tag.Value.StartsWith("meadow") || tag.Value.StartsWith("grass") 
                            || tag.Value.StartsWith("greenfiel") || tag.Value.StartsWith("recreation_ground") || tag.Value.StartsWith("winter_sports")
                            || tag.Value.StartsWith("allotments")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.grass, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("reservoir") || tag.Value.StartsWith("basin")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.reservoir, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("2")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.two, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("yes")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.yes, "");
                            featureData.PropertyValues.values.Add(structValuesProp);
                        }
                        else { 
                            featureData.PropertyKeys.keys.RemoveAt(featureData.PropertyKeys.keys.Count()-1);
                        }
                    
                    }

                }
                foreach (var nodeId in way.NodeIds)
                {
                    var node = mapData.Nodes[nodeId];
                    if (TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude)) != tileId)
                    {
                        continue;
                    }

                    usedNodes.Add(nodeId);

                    foreach (var (key, value) in node.Tags)
                    {
                        
                        added = true;
                        if (key.StartsWith("natural")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.natural);
                        }
                        else if (key.StartsWith("place")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.place);
                        }
                        else if (key.StartsWith("boundary")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.boundary);
                        }
                        else if (key.StartsWith("admin_level")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.admin_level);
                        }
                        else if (key.StartsWith("highway")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.highway);
                        }
                        else if (key.StartsWith("water")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.water);
                        }
                        else if (key.StartsWith("railway")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.railway);
                        }
                        else if (key.StartsWith("landuse")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.landuse);
                        }
                        else if (key.StartsWith("name")) { 
                            labels[^1] = totalPropertyCount * 2 + featureData.PropertyKeys.keys.Count * 2 + 1;
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.name);
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.none, value);
                            featureData.PropertyValues.values.Add(structValuesProp);
                            added = false;
                        
                        }
                        else if (key.StartsWith("leisure")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.leisure);
                        }
                        else if (key.StartsWith("amenity")) { 
                            featureData.PropertyKeys.keys.Add(MapFeatureData.EnumKeysProp.amenity);
                        }
                        else { added = false;}

                        if (added) { 
                            if (value.StartsWith("fell") || value.StartsWith("grassland") || value.StartsWith("heath") || value.StartsWith("moor")
                                || value.StartsWith("scrub") || value.StartsWith("wetland")) { 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.fell, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("wood") || value.StartsWith("tree_row")) { 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.wood, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("bare_rock") || value.StartsWith("rock") || value.StartsWith("scree")) { 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.rock, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("beach") || value.StartsWith("sand") ) { 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.sand, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("water")) { 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.water, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("city") || value.StartsWith("town") || value.StartsWith("locality") 
                                || value.StartsWith("hamlet")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.city, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("motorway") || value.StartsWith("trunk") || value.StartsWith("primary") 
                                || value.StartsWith("secondary") || value.StartsWith("tertiary") || value.StartsWith("road")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.road, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if ( value.StartsWith("administrative")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.administrative, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("forest") || value.StartsWith("orchad")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.forest, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("residential") || value.StartsWith("cemetery") || value.StartsWith("industrial") 
                                ||value.StartsWith("commercial") || value.StartsWith("square") || value.StartsWith("construction")
                                || value.StartsWith("military")|| value.StartsWith("quarry")|| value.StartsWith("brownfield")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.construction, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("farm") || value.StartsWith("meadow") || value.StartsWith("grass") 
                                || value.StartsWith("greenfiel") || value.StartsWith("recreation_ground") || value.StartsWith("winter_sports")
                                || value.StartsWith("allotments")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.grass, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("reservoir") || value.StartsWith("basin")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.reservoir, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("2")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.two, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else if (value.StartsWith("yes")){ 
                                MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.yes, "");
                                featureData.PropertyValues.values.Add(structValuesProp);
                            }
                            else { 
                                featureData.PropertyKeys.keys.RemoveAt(featureData.PropertyKeys.keys.Count()-1);
                            }
                    
                        }
                    }



                    featureData.Coordinates.coordinates.Add(new Coordinate(node.Latitude, node.Longitude));
                
                
                }

                // This feature is not located within this tile, skip it
                if (featureData.Coordinates.coordinates.Count == 0)
                {
                    // Remove the last item since we added it preemptively
                    labels.RemoveAt(labels.Count - 1);
                    continue;
                }

                if (featureData.Coordinates.coordinates[0] == featureData.Coordinates.coordinates[^1])
                {
                    geometryType = GeometryType.Polygon;
                }
                featureData.GeometryType = (byte)geometryType;

                totalPropertyCount += featureData.PropertyKeys.keys.Count;
                totalCoordinateCount += featureData.Coordinates.coordinates.Count;

                if (featureData.PropertyKeys.keys.Count != featureData.PropertyValues.values.Count)
                {
                    throw new InvalidDataContractException("Property keys and values should have the same count");
                }

                featureIds.Add(featureId);
                featuresData.Add(featureId, featureData);
            }

            foreach (var (nodeId, node) in mapData.Nodes.Where(n => !usedNodes.Contains(n.Key)))
            {   
                
                if (TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude)) != tileId)
                {
                    continue;
                }

                var featureId = Interlocked.Increment(ref featureIdCounter);

                var featurePropKeys = new List<MapFeatureData.EnumKeysProp>();
                var featurePropValues = new List<MapFeatureData.StructValuesProp>();
                var added = true;

                labels.Add(-1);
                for (var i = 0; i < node.Tags.Count; ++i)
                {
                    var tag = node.Tags[i];
                    if (tag.Key.StartsWith("natural")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.natural);
                    }
                    else if (tag.Key.StartsWith("place")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.place);
                    }
                    else if (tag.Key.StartsWith("boundary")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.boundary);
                    }
                    else if (tag.Key.StartsWith("admin_level")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.admin_level);
                    }
                    else if (tag.Key.StartsWith("highway")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.highway);
                    }
                    else if (tag.Key.StartsWith("water")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.water);
                    }
                    else if (tag.Key.StartsWith("railway")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.railway);
                    }
                    else if (tag.Key.StartsWith("landuse")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.landuse);
                    }
                    else if (tag.Key.StartsWith("name")) { 
                        labels[^1] = totalPropertyCount * 2 + featurePropKeys.Count * 2 + 1;
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.name);
                        MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.none, tag.Value);
                        featurePropValues.Add(structValuesProp);
                        added = false;
                        
                    }
                    else if (tag.Key.StartsWith("leisure")) { 
                       featurePropKeys.Add(MapFeatureData.EnumKeysProp.leisure);
                    }
                    else if (tag.Key.StartsWith("amenity")) { 
                        featurePropKeys.Add(MapFeatureData.EnumKeysProp.amenity);
                    }
                    else { added = false;}

                    if (added) { 
                        if (tag.Value.StartsWith("fell") || tag.Value.StartsWith("grassland") || tag.Value.StartsWith("heath") || tag.Value.StartsWith("moor")
                            || tag.Value.StartsWith("scrub") || tag.Value.StartsWith("wetland")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.fell, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("wood") || tag.Value.StartsWith("tree_row")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.wood, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("bare_rock") || tag.Value.StartsWith("rock") || tag.Value.StartsWith("scree")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.rock, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("beach") || tag.Value.StartsWith("sand") ) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.sand, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("water")) { 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.water, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("city") || tag.Value.StartsWith("town") || tag.Value.StartsWith("locality") 
                            || tag.Value.StartsWith("hamlet")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.city, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("motorway") || tag.Value.StartsWith("trunk") || tag.Value.StartsWith("primary") 
                            || tag.Value.StartsWith("secondary") || tag.Value.StartsWith("tertiary") || tag.Value.StartsWith("road")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.road, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if ( tag.Value.StartsWith("administrative")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.administrative, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("forest") || tag.Value.StartsWith("orchad")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.forest, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("residential") || tag.Value.StartsWith("cemetery") || tag.Value.StartsWith("industrial") 
                            || tag.Value.StartsWith("commercial") || tag.Value.StartsWith("square") || tag.Value.StartsWith("construction")
                            || tag.Value.StartsWith("military")|| tag.Value.StartsWith("quarry")|| tag.Value.StartsWith("brownfield")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.construction, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("farm") || tag.Value.StartsWith("meadow") || tag.Value.StartsWith("grass") 
                            || tag.Value.StartsWith("greenfiel") || tag.Value.StartsWith("recreation_ground") || tag.Value.StartsWith("winter_sports")
                            || tag.Value.StartsWith("allotments")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.grass, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("reservoir") || tag.Value.StartsWith("basin")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.reservoir, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("2")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.two, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else if (tag.Value.StartsWith("yes")){ 
                            MapFeatureData.StructValuesProp structValuesProp = new MapFeatureData.StructValuesProp(MapFeatureData.StructValuesProp.EnumValuesProp.yes, "");
                            featurePropValues.Add(structValuesProp);
                        }
                        else { 
                            featurePropKeys.RemoveAt(featurePropKeys.Count()-1);
                        }
                    
                    }
                }

                if (featurePropKeys.Count != featurePropValues.Count)
                {
                    throw new InvalidDataContractException("Property keys and values should have the same count");
                }

                var fData = new FeatureData
                {
                    Id = featureId,
                    GeometryType = (byte)GeometryType.Point,
                    Coordinates = (totalCoordinateCount, new List<Coordinate>
                    {
                        new Coordinate(node.Latitude, node.Longitude)
                    }),
                   
                    PropertyKeys = (totalPropertyCount, featurePropKeys),
                    PropertyValues = (totalPropertyCount, featurePropValues)
                };
                featuresData.Add(featureId, fData);
                featureIds.Add(featureId);

                totalPropertyCount += featurePropKeys.Count;
                ++totalCoordinateCount;
            }

            offsets.Add(tileId, fileWriter.BaseStream.Position);

            // Write TileBlockHeader
            fileWriter.Write(featureIds.Count); // TileBlockHeader: FeatureCount
            fileWriter.Write(totalCoordinateCount); // TileBlockHeader: CoordinateCount
            fileWriter.Write(totalPropertyCount * 2); // TileBlockHeader: StringCount
            fileWriter.Write(0); //TileBlockHeader: CharactersCount

            // Take note of the offset within the file for this field
            var coPosition = fileWriter.BaseStream.Position;
            // Write a placeholder value to reserve space in the file
            fileWriter.Write((long)0); // TileBlockHeader: CoordinatesOffsetInBytes (placeholder)

            // Take note of the offset within the file for this field
            var soPosition = fileWriter.BaseStream.Position;
            // Write a placeholder value to reserve space in the file
            fileWriter.Write((long)0); // TileBlockHeader: StringsOffsetInBytes (placeholder)

            // Take note of the offset within the file for this field
            var choPosition = fileWriter.BaseStream.Position;
            // Write a placeholder value to reserve space in the file
            fileWriter.Write((long)0); // TileBlockHeader: CharactersOffsetInBytes (placeholder)

            // Write MapFeatures
            for (var i = 0; i < featureIds.Count; ++i)
            {
                var featureData = featuresData[featureIds[i]];

                fileWriter.Write(featureIds[i]); // MapFeature: Id
                fileWriter.Write(labels[i]); // MapFeature: LabelOffset
                fileWriter.Write(featureData.GeometryType); // MapFeature: GeometryType
                fileWriter.Write(featureData.Coordinates.offset); // MapFeature: CoordinateOffset
                fileWriter.Write(featureData.Coordinates.coordinates.Count); // MapFeature: CoordinateCount
                
                fileWriter.Write(featureData.PropertyKeys.offset * 2); // MapFeature: PropertiesOffset 
                fileWriter.Write(featureData.PropertyKeys.keys.Count); // MapFeature: PropertyCount
            }

            // Record the current position in the stream
            var currentPosition = fileWriter.BaseStream.Position;
            // Seek back in the file to the position of the field
            fileWriter.BaseStream.Position = coPosition;
            // Write the recorded 'currentPosition'
            fileWriter.Write(currentPosition); // TileBlockHeader: CoordinatesOffsetInBytes
            // And seek forward to continue updating the file
            fileWriter.BaseStream.Position = currentPosition;
            foreach (var t in featureIds)
            {
                var featureData = featuresData[t];

                foreach (var c in featureData.Coordinates.coordinates)
                {
                    fileWriter.Write(c.Latitude); // Coordinate: Latitude
                    fileWriter.Write(c.Longitude); // Coordinate: Longitude
                }
            }

            // Record the current position in the stream
            currentPosition = fileWriter.BaseStream.Position;
            // Seek back in the file to the position of the field
            fileWriter.BaseStream.Position = soPosition;
            // Write the recorded 'currentPosition'
            fileWriter.Write(currentPosition); // TileBlockHeader: StringsOffsetInBytes
            // And seek forward to continue updating the file
            fileWriter.BaseStream.Position = currentPosition;

            var stringOffset = 0;
            foreach (var t in featureIds)
            {
                var featureData = featuresData[t];
                for (var i = 0; i < featureData.PropertyKeys.keys.Count; ++i)
                {
                    ReadOnlySpan<char> k = Convert.ToString(featureData.PropertyKeys.keys[i]);

                    ReadOnlySpan<char> v;

                    if (featureData.PropertyValues.values[i].name == "")
                    {
                        v = featureData.PropertyValues.values[i].PropValues.ToString();
                    }
                    else
                    {
                        v = featureData.PropertyValues.values[i].name.ToString();
                    }


                    fileWriter.Write(stringOffset); // StringEntry: Offset
                    fileWriter.Write(k.Length); // StringEntry: Length
                    stringOffset += k.Length;

                    fileWriter.Write(stringOffset); // StringEntry: Offset
                    fileWriter.Write(v.Length); // StringEntry: Length
                    stringOffset += v.Length;
                }
            }

            // Record the current position in the stream
            currentPosition = fileWriter.BaseStream.Position;
            // Seek back in the file to the position of the field
            fileWriter.BaseStream.Position = choPosition;
            // Write the recorded 'currentPosition'
            fileWriter.Write(currentPosition); // TileBlockHeader: CharactersOffsetInBytes
            // And seek forward to continue updating the file
            fileWriter.BaseStream.Position = currentPosition;
            foreach (var t in featureIds)
            {
                var featureData = featuresData[t];
                for (var i = 0; i < featureData.PropertyKeys.keys.Count; ++i)
                {
                    ReadOnlySpan<char> k = Convert.ToString(featureData.PropertyKeys.keys[i]);
                    foreach (var c in k)
                    {
                        fileWriter.Write((short)c);
                    }

                    ReadOnlySpan<char> v;

                    if (featureData.PropertyValues.values[i].name == "")
                    {
                        v = featureData.PropertyValues.values[i].PropValues.ToString();
                    }
                    else
                    {
                        v = featureData.PropertyValues.values[i].name.ToString();
                    }
                    foreach (var c in v)
                    {
                        fileWriter.Write((short)c);
                    }
                }
            }
        }

        // Seek to the beginning of the file, just before the first TileHeaderEntry
        fileWriter.Seek(Marshal.SizeOf<FileHeader>(), SeekOrigin.Begin);
        foreach (var (tileId, offset) in offsets)
        {
            fileWriter.Write(tileId);
            fileWriter.Write(offset);
        }

        fileWriter.Flush();
    }

    public static void Main(string[] args)
    {
        Options? arguments = null;
        var argParseResult =
            Parser.Default.ParseArguments<Options>(args).WithParsed(options => { arguments = options; });

        if (argParseResult.Errors.Any())
        {
            Environment.Exit(-1);
        }

        var mapData = LoadOsmFile(arguments!.OsmPbfFilePath);
        CreateMapDataFile(ref mapData, arguments!.OutputFilePath!);
    }

    public class Options
    {
        [Option('i', "input", Required = true, HelpText = "Input osm.pbf file")]
        public string? OsmPbfFilePath { get; set; }

        [Option('o', "output", Required = true, HelpText = "Output binary file")]
        public string? OutputFilePath { get; set; }
    }

    private readonly struct MapData
    {
        public ImmutableDictionary<long, AbstractNode> Nodes { get; init; }
        public ImmutableDictionary<int, List<long>> Tiles { get; init; }
        public ImmutableArray<Way> Ways { get; init; }
    }

    private struct FeatureData
    {
        public long Id { get; init; }

        public byte GeometryType { get; set; }
        public (int offset, List<Coordinate> coordinates) Coordinates { get; init; }
        public (int offset, List<MapFeatureData.EnumKeysProp> keys) PropertyKeys { get; init; }
        public (int offset, List<MapFeatureData.StructValuesProp> values) PropertyValues { get; init; }
    }
}
