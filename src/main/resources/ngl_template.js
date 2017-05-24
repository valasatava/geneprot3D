
// Template

var structure = "rcsb://3hn3.mmtf";

stage.loadFile(structure).then( function( o ){

    // show protein
    o.addRepresentation( "cartoon", {
        sele: ":A",
        color: "skyblue"
    } );

    // select exon 1
    o.addRepresentation( "cartoon", {
        sele: ":A and (159-355)",
        color: "green"
    } );

    // select exon 1
    o.addRepresentation( "cartoon", {
        sele: ":A and (464-492)",
        color: "blue"
    } );

    // residues that are close in 3D space
    o.addRepresentation( "ball+stick", {
      sele: "329:A or 477:A",
      color: "red",
    } );

    // atoms that are close in 3D space
    o.addRepresentation( "distance", {
    atomPair: [
        [ "329:A.CA", "477:A.CA" ],
    ],
    scale: 0.5,
    color: "element",
    labelVisible: true
} );

} );
