
// Template

var structure = "rcsb://1zd3.mmtf";

stage.loadFile(structure).then( function( o ){

    // show protein
    o.addRepresentation( "cartoon", {
        sele: ":A",
        color: "skyblue"
    } );

    // select exon 1
    o.addRepresentation( "cartoon", {
        sele: ":A and (276-302)",
        color: "green"
    } );

    // select exon 1
    o.addRepresentation( "cartoon", {
        sele: ":A and (415-425)",
        color: "blue"
    } );

    // residues that are close in 3D space
    o.addRepresentation( "ball+stick", {
      sele: "294:A or 418:A",
      color: "red",
    } );

    // atoms that are close in 3D space
    o.addRepresentation( "distance", {
        atomPair: [
            [ "294:A.CA", "418:A.CA" ],
        ],
        scale: 0.5,
        color: "element",
        labelVisible: true
    } );

} );
