
// Template to visualize structural data using NGL viewer

var structure = "${source}";

stage.loadFile(structure).then( function( o ){

    // show protein
    o.addRepresentation( "cartoon", {
        sele: ":${chain}",
        color: "skyblue"
    } );

    // select exon 1
    o.addRepresentation( "cartoon", {
        sele: ":${chain} and (${start1}-${end1})",
        color: "green"
    } );

    // select exon 1
    o.addRepresentation( "cartoon", {
        sele: ":${chain} and (${start2}-${end2})",
        color: "blue"
    } );

    // residues that are close in 3D space
    o.addRepresentation( "ball+stick", {
      sele: "${resn1}:${chain} or ${resn2}:${chain}",
      color: "red",
    } );

    // atoms that are close in 3D space
    o.addRepresentation( "distance", {
        atomPair: [
            [ "${resn1}:${chain}.CA", "${resn2}:${chain}.CA" ],
        ],
        scale: 0.5,
        color: "element",
        labelVisible: true
    } );

} );
