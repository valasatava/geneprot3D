
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
      sele: "${distRes1}:${chain}",
      color: "green",
    } );
    o.addRepresentation( "ball+stick", {
        sele: "${distRes2}:${chain}",
        color: "blue",
    } );

    // atoms that are close in 3D space
    o.addRepresentation( "distance", {
        atomPair: [
            [ "${distRes1}:${chain}.CA", "${distRes2}:${chain}.CA" ],
        ],
        scale: 0.5,
        color: "element",
        labelVisible: true
    } );

    // active sites residues
    "${activeSitesSelection}"

    stage.autoView();

} );
