    with donor_diseases as
    (
    select
    ds.id as donor_id
    , array_to_string(array_agg(mc.description), ' ') as diseases
    from donors ds
    join donor_medical_conditions dmc on dmc.donor_id = ds.id
    join medical_conditions mc on mc.id = dmc.medical_condition_id
    group by ds.id
    ),

    cells as (
    select distinct
    cell.name as cell_specimen_name
    ,dnr.external_donor_name as labtracks_id
    ,cell.ephys_roi_result_id
    ,cell.patched_cell_container
    ,slice.histology_well_name as slice_histology_well
    ,bw.name as slice_biocytin_well
    ,hem.name as hemisphere
    ,tsop.name as tcp_sop
    ,err.workflow_state as ephys_qc
    ,err.recording_date
    ,err.published_at
    ,cellr.name as cell_reporter
    ,cell.id as cell_specimen_id
    ,cell.parent_id as slice_specimen_id 
    ,flip.name as flipped
    ,slice.parent_id as slab_specimen_id 
    ,cell.project_id
    ,cell.donor_id
    ,age.name as age
    ,str.acronym as structure
    ,layer.acronym as layer
    ,err.seal_gohm
    ,dds.diseases as disease_state
    from specimens cell
    join specimens slice on cell.parent_id = slice.id
    join flipped_specimens flip on flip.id = slice.flipped_specimen_id
    left join hemispheres hem on hem.id = slice.hemisphere_id
    left join biocytin_wells bw on bw.specimen_id = slice.id
    join tissue_processings tp on slice.tissue_processing_id = tp.id
    join task_templates tpt on tp.tissue_processing_template_id = tpt.id and tpt.type = 'TissueProcessingTemplate'
    left join tcp_sops tsop on tpt.tcp_sop_id = tsop.id
    left join cell_reporters cellr on cell.cell_reporter_id = cellr.id
    join ephys_roi_results err on err.id = cell.ephys_roi_result_id
    join donors dnr on cell.donor_id = dnr.id
    left join donor_diseases dds on dds.donor_id = dnr.id
    left join ages age on dnr.age_id = age.id
    left join structures str on cell.structure_id = str.id
    left join structures layer on layer.id = cell.cortex_layer_id
    ),

    reporters as (
    select
    dg.donor_id
    ,array_to_string(array_agg(DISTINCT g.name), ' ') as reporters
    from donors_genotypes dg
    join genotypes g on dg.genotype_id = g.id
    join genotype_types gt on g.genotype_type_id = gt.id
    where gt.name ilike 'reporter%'
    group by dg.donor_id
    ),

    drivers as (
    select
    dg.donor_id
    ,array_to_string(array_agg(DISTINCT g.name), ' ') as drivers
    from donors_genotypes dg
    join genotypes g on dg.genotype_id = g.id
    join genotype_types gt on g.genotype_type_id = gt.id
    where gt.name ilike 'driver%'
    group by dg.donor_id
    ),

    cell_soma_location as (
    select distinct specimen_id as cell_id, z as pin_z, soma_depth_um as depth_from_pia from cell_soma_locations
    ),

    dendrite_type as (
    select specimen_id, 'dendrite type - ' || dt.name as dendrite_type
    from task_templates tt
    join dendrite_types dt
    on tt.dendrite_type_id = dt.id
    where tt.type = 'MorphologyEvalTask'
    ),

    dendrite_type_old as (
    select
    sts.specimen_id
    ,array_to_string(array_agg(DISTINCT tag.name), ' ') as dendrite_type
    from specimen_tags_specimens sts
    join specimen_tags tag on sts.specimen_tag_id = tag.id
    where tag.name in('dendrite type - spiny', 'dendrite type - NA', 'dendrite type - sparsely spiny', 'dendrite type - aspiny')
    group by sts.specimen_id
    ),

    apical_old as (
    select
    sts.specimen_id
    ,array_to_string(array_agg(DISTINCT tag.name), ' ') as apical
    from specimen_tags_specimens sts
    join specimen_tags tag on sts.specimen_tag_id = tag.id
    where tag.name ilike 'apical%'
    group by sts.specimen_id
    ),

    apical as (
    select specimen_id, 'apical - ' || a.name as apical
    from task_templates tt
    join apical_dendrite_states a
    on tt.apical_dendrite_state_id = a.id
    where tt.type = 'MorphologyEvalTask'
    ),

    nwbd as (
    select wkf.attachable_id as ephys_roi_result_id, wkf.storage_directory || max(wkf.filename) as nwb_filename
    from well_known_files wkf
    join well_known_file_types wkft on wkf.well_known_file_type_id = wkft.id and wkft.name in('NWBDownload')
    group by wkf.attachable_id, wkf.storage_directory
    ),

    pub_validation as (
    select j.enqueued_object_id as ephys_roi_result_id, js.name as job_state
    from jobs j
    join job_states js on j.job_state_id = js.id
    join job_queues jq on j.job_queue_id = jq.id and jq.name = 'EPHYS_PUBLISH_VALIDATION_QUEUE'
    where j.archived = false
    ),

    swc as (
    select nr.specimen_id as cell_id, swc.storage_directory || swc.filename as swc_filename, marker.storage_directory || marker.filename as marker_filename, swc.published_at as published_at
    from neuron_reconstructions nr
    join well_known_files swc on swc.attachable_id = nr.id and swc.well_known_file_type_id = 303941301
    left join well_known_files marker on marker.attachable_id = nr.id and marker.well_known_file_type_id = 486753749
    where nr.superseded = false and nr.manual = true
    ),

    imgs63 as (
    with tag_list as (
    select distinct ims.id as ims_id, imst.name as tag from image_series ims
    join image_series_image_series_tags ims2imst on ims2imst.image_series_id = ims.id
    join image_series_tags imst on imst.id = ims2imst.image_series_tag_id
    order by 1,2
    ),

    max_sti as (
    select distinct ims.id as ims_id, max(specimen_tissue_index) as pos_in_spec from image_series ims
    join sub_images on ims.id = sub_images.image_series_id
    group by ims.id
    )
    select max(id) as image_series_id, max(workflow_state) as workflow_state, specimen_id, max(max_sti.pos_in_spec) as pos_in_spec, array_to_string(array_agg(DISTINCT tag_list.tag), '_AND_') as tags
    from image_series ims
    left join tag_list on tag_list.ims_id = ims.id
    left join max_sti on ims.id = max_sti.ims_id
    where is_stack = true
    group by specimen_id
    ),

    imgs20 as (
    with tag_list as (
    select distinct ims.id as ims_id, imst.name as tag from image_series ims
    join image_series_image_series_tags ims2imst on ims2imst.image_series_id = ims.id
    join image_series_tags imst on imst.id = ims2imst.image_series_tag_id
    order by 1,2
    )
    select distinct ims.id as image_series_id, ims.workflow_state as workflow_state, ims.specimen_id, array_to_string(array_agg(DISTINCT tag_list.tag), '_AND_') as tags from image_series ims
    left join tag_list on tag_list.ims_id = ims.id
    where ims.type = 'FocalPlaneImageSeries' and is_stack = false
    group by ims.id, ims.workflow_state, ims.specimen_id

    ), mip as (
    select distinct sp.id as specimen_id, si.id as mip_id from specimens sp
    join sub_images si on sp.id = si.specimen_id
    join images im on si.image_id = im.id
    where im.image_type_id = 310980861

    ),

    psw as (
    with pia as (
    select distinct cell.id as cell_id, p.id as p_id from specimens cell
    join specimens slice on slice.id = cell.parent_id
    join image_series ims20 on ims20.specimen_id = slice.id and ims20.type = 'FocalPlaneImageSeries' and ims20.is_stack is false
    join sub_images si on si.image_series_id = ims20.id
    join biospecimen_polygons bpp on bpp.biospecimen_id = cell.id
    join avg_graphic_objects p on p.id = bpp.polygon_id
    join avg_graphic_objects player on player.id = p.parent_id and player.group_label_id = 304999940 and player.sub_image_id = si.id
    ),

    soma as (
    select distinct cell.id as cell_id, s.id as s_id from specimens cell
    join specimens slice on slice.id = cell.parent_id
    join image_series ims20 on ims20.specimen_id = slice.id and ims20.type = 'FocalPlaneImageSeries' and ims20.is_stack is false
    join sub_images si on si.image_series_id = ims20.id
    join biospecimen_polygons bps on bps.biospecimen_id = cell.id
    join avg_graphic_objects s on s.id = bps.polygon_id
    join avg_graphic_objects slayer on slayer.id = s.parent_id and slayer.group_label_id = 306931999 and slayer.sub_image_id = si.id
    ),

    wm as (
    select distinct cell.id as cell_id, w.id as w_id from specimens cell
    join specimens slice on slice.id = cell.parent_id
    join image_series ims20 on ims20.specimen_id = slice.id and ims20.type = 'FocalPlaneImageSeries' and ims20.is_stack is false
    join sub_images si on si.image_series_id = ims20.id
    join biospecimen_polygons bpw on bpw.biospecimen_id = cell.id
    join avg_graphic_objects w on w.id = bpw.polygon_id
    join avg_graphic_objects wlayer on wlayer.id = w.parent_id and wlayer.group_label_id = 304999941 and wlayer.sub_image_id = si.id
    )
    select distinct cell.id as cell_id,
    case when pia.p_id is not null then 'p' else '' end ||
    case when soma.s_id is not null then 's' else '' end ||
    case when wm.w_id is not null then 'w' else '' end as annotations
    from specimens cell
    left join pia on pia.cell_id = cell.id
    left join soma on soma.cell_id = cell.id
    left join wm on wm.cell_id = cell.id
    ),

    cortlayer as (
    with list as (
    select distinct ims20.id as ims20_id, clayer.acronym as acronym from image_series ims20
    join sub_images si on si.image_series_id = ims20.id
    join avg_graphic_objects layer on layer.sub_image_id = si.id and layer.group_label_id = 552251970
    join avg_graphic_objects poly on poly.parent_id = layer.id
    join structures clayer on clayer.id = poly.cortex_layer_id
    order by 1,2
    )
    select distinct ims20_id, array_to_string(array_agg(DISTINCT list.acronym), '_AND_') as annotations from list
    group by ims20_id
    ),

    mthmb as (
    select distinct attachable_id as ephys_roi_result_id
    from well_known_files
    where well_known_file_type_id = (select id from well_known_file_types where name = 'MorphologyThumbnail')
    and attachable_type = 'EphysRoiResult'
    ),

    eitthmb as (
    select distinct attachable_id as ephys_roi_result_id
    from well_known_files
    where well_known_file_type_id = (select id from well_known_file_types where name = 'EphysInstantaneousThresholdThumbnail')
    and attachable_type = 'EphysRoiResult'
    ),

    esthmb as (
    select distinct attachable_id as ephys_roi_result_id
    from well_known_files
    where well_known_file_type_id = (select id from well_known_file_types where name = 'EphysSummaryThumbnail')
    and attachable_type = 'EphysRoiResult'
    ),

    glif1 as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id and nmtg1.name = '1 Leaky Integrate and Fire (LIF)'
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    glif2 as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id and nmtg1.name = '2 LIF + Reset Rules (LIF-R)'
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    glif3 as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id and nmtg1.name = '3 LIF + Afterspike Currents (LIF-ASC)'
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    glif4 as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id and nmtg1.name = '4 LIF-R + Afterspike Currents (LIF-R-ASC)'
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    glif5 as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id and nmtg1.name = '5 LIF-R-ASC + Threshold Adaptation (LIF-R-ASC-A)'
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    biophys as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id and nmtg1.name = 'Biophysical - perisomatic'
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    actden as (
    select specimen_id, count(nmr.id) as cnt
    from neuronal_models nm
    join neuronal_model_templates nmtg1 on nm.neuronal_model_template_id = nmtg1.id
    and nmtg1.id = 491455321 
    join neuronal_model_runs nmr on nm.id = nmr.neuronal_model_id and nmr.workflow_state = 'passed'
    group by nm.specimen_id
    ),

    do_63x(cell_id, go) as (
    select distinct cell.id, array_to_string(array_agg(DISTINCT tag.name), ' _AND_ ') from specimens cell
    join ephys_roi_results err on err.id = cell.ephys_roi_result_id
    join specimen_tags_specimens sptagsp on sptagsp.specimen_id = cell.id
    join specimen_tags tag on tag.id = sptagsp.specimen_tag_id and tag.id in (602120185,602122082)
    group by cell.id
    order by 1
    ), started_63x(cell_id, started) as (
    select distinct cell.id, tag.name from specimens cell
    join ephys_roi_results err on err.id = cell.ephys_roi_result_id
    join specimen_tags_specimens sptagsp on sptagsp.specimen_id = cell.id
    join specimen_tags tag on tag.id = sptagsp.specimen_tag_id and tag.id = 646831639
    order by 1
    ), check_slide(cell_id, value) as (
    select distinct cell.id, tag.name from specimens cell
    join ephys_roi_results err on err.id = cell.ephys_roi_result_id
    join specimen_tags_specimens sptagsp on sptagsp.specimen_id = cell.id
    join specimen_tags tag on tag.id = sptagsp.specimen_tag_id and tag.id = 647518278
    order by 1
    ),

    na_objective(ims_id, value) as (
    select distinct ims.id, tag.name from image_series ims
    join image_series_image_series_tags imsimst on ims.id = imsimst.image_series_id
    join image_series_tags tag on imsimst.image_series_tag_id = tag.id
    where tag.id = 679471263
    order by 1
    ),

    cell_type(cell_id, cell_type) as (
    select distinct cell.id, array_to_string(array_agg(DISTINCT t.name), ' _AND_ ') from specimens cell
    join ephys_roi_results err on err.id = cell.ephys_roi_result_id
    join specimen_types_specimens sptsp on sptsp.specimen_id = cell.id
    join specimen_types t on t.id = sptsp.specimen_type_id and t.id in (305008011, 565468110)
    group by cell.id
    order by 1
    ),

    tissue_reviews as (
    select specimen_id, count(*) as tissue_reviews
    from specimen_metadata
    where kind = 'IVSCC tissue review'
    group by specimen_id
    )

    select distinct
    proj.code as project
    ,cell.cell_specimen_id
    ,'http://lims2/specimens?id=' || cell.cell_specimen_id as link_cell
    ,cell.cell_specimen_name as cell
    ,cell_type.cell_type
    ,cell.hemisphere
    ,cell.flipped
    ,'http://lims2/specimens?search[name]=' || replace(slice.name,';', '%3B') as link_slice
    ,cell.slice_histology_well
    ,cell.slice_biocytin_well
    ,sl20x.barcode
    ,cell.slice_specimen_id
    ,cell.patched_cell_container
    ,cell.ephys_roi_result_id
    ,cell.recording_date
    ,cell.seal_gohm
    ,cell.tcp_sop
    ,cell.ephys_qc
    ,ras.name
    ,case when ra.id is not null then (case when ra.failed = 't' then 'failed' else 'passed' end) else '' end as rna_amp_state
    ,cell.published_at
    ,cell.labtracks_id
    ,rpt.reporters
    ,drv.drivers
    ,cell.cell_reporter
    ,case when tr.tissue_reviews is not null and tr.tissue_reviews > 0 then 'reviewed' else '' end as has_tissue_review
    ,cell.structure
    ,cell.layer
    ,dt_old.dendrite_type as dendrite_type_tag_old
    ,a_old.apical as apical_tag_old
    ,dt.dendrite_type as dendrite_type_tag
    ,a.apical as apical_tag
    ,cell_soma_location.pin_z
    ,case when pub_validation.job_state = 'SUCCESS' then 'nwb ready' else '' end as nwb_ready
    ,case when nwbd.ephys_roi_result_id is not null then 'nwb d' else '' end as has_nwb_download
    ,imgs20.image_series_id as image_series_20x_id
    ,imgs20.workflow_state as image_series_20x_qc
    ,psw.annotations as psw_annotations
    ,cortlayer.annotations as layer_annotations
    ,imgs63.tags
    ,imgs63.pos_in_spec
    ,'http://lims2/focal_plane_image_series?id=' || imgs20.image_series_id as link_20x
    ,check_slide.value
    ,do_63x.go
    ,started_63x.started
    ,imgs63.image_series_id as image_series_63x_id
    ,imgs63.workflow_state as image_series_63x_qc
    ,'http://lims2/focal_plane_image_series?id=' || imgs63.image_series_id as link_63x
    ,case when mthmb.ephys_roi_result_id is not null then 'morph thumb' else '' end as has_morph_thumb
    , thumb.storage_directory || thumb.filename as MorphologyThumbnail
    , hres_thumb.storage_directory || hres_thumb.filename as HighResMorphologyThumbnail
    ,case when eitthmb.ephys_roi_result_id is not null then 'inst thres thumb' else '' end as has_inst_thres_thumb
    ,case when esthmb.ephys_roi_result_id is not null then 'sum thumb' else '' end as has_sum_thumb
    ,case when glif1.cnt > 0 then 'glif1' else '' end as has_glif1
    ,case when glif2.cnt > 0 then 'glif2' else '' end as has_glif2
    ,case when glif3.cnt > 0 then 'glif3' else '' end as has_glif3
    ,case when glif4.cnt > 0 then 'glif4' else '' end as has_glif4
    ,case when glif5.cnt > 0 then 'glif5' else '' end as has_glif5
    ,case when biophys.cnt > 0 then 'biophys' else '' end as has_biophysical
    ,case when actden.cnt > 0 then 'AD' else '' end as has_active_dendrite
    ,nwbd.nwb_filename
    ,swc.swc_filename
    ,swc.marker_filename
    ,swc.published_at
    ,cell.disease_state
    ,'http://lims2/siv?sub_image=' || mip.mip_id as mip_link
    ,cell_soma_location.depth_from_pia as depth_from_pia
    ,na_objective.value

    from cells cell
    join specimens slice on slice.id = cell.slice_specimen_id
    join projects proj on cell.project_id = proj.id
    left join cell_soma_location on cell.cell_specimen_id = cell_soma_location.cell_id
    left join reporters rpt on cell.donor_id = rpt.donor_id
    left join drivers drv on cell.donor_id = drv.donor_id
    left join dendrite_type_old dt_old on cell.cell_specimen_id = dt_old.specimen_id
    left join apical_old a_old on cell.cell_specimen_id = a_old.specimen_id
    left join dendrite_type dt on cell.cell_specimen_id = dt.specimen_id
    left join apical a on cell.cell_specimen_id = a.specimen_id
    left join nwbd on cell.ephys_roi_result_id = nwbd.ephys_roi_result_id
    left join swc on cell.cell_specimen_id = swc.cell_id
    left join pub_validation on cell.ephys_roi_result_id = pub_validation.ephys_roi_result_id
    left join imgs20 on cell.slice_specimen_id = imgs20.specimen_id
    left join mip on cell.cell_specimen_id = mip.specimen_id
    left join psw on psw.cell_id = cell.cell_specimen_id
    left join imgs63 on cell.cell_specimen_id = imgs63.specimen_id
    left join mthmb on cell.ephys_roi_result_id = mthmb.ephys_roi_result_id
    left join eitthmb on cell.ephys_roi_result_id = eitthmb.ephys_roi_result_id
    left join esthmb on cell.ephys_roi_result_id = esthmb.ephys_roi_result_id
    left join glif1 on cell.cell_specimen_id = glif1.specimen_id
    left join glif2 on cell.cell_specimen_id = glif2.specimen_id
    left join glif3 on cell.cell_specimen_id = glif3.specimen_id
    left join glif4 on cell.cell_specimen_id = glif4.specimen_id
    left join glif5 on cell.cell_specimen_id = glif5.specimen_id
    left join biophys on cell.cell_specimen_id = biophys.specimen_id
    left join actden on cell.cell_specimen_id = actden.specimen_id
    left join cell_type on cell_type.cell_id = cell.cell_specimen_id
    left join rna_amplification_inputs rai on rai.sample_id = cell.cell_specimen_id
    left join rna_amplifications ra on ra.id = rai.rna_amplification_id
    left join rna_amplification_sets ras on ras.id = ra.rna_amplification_set_id
    left join do_63x on do_63x.cell_id = cell.cell_specimen_id
    left join started_63x on started_63x.cell_id = cell.cell_specimen_id
    left join image_series_slides imss on imss.image_series_id = imgs20.image_series_id
    left join slides sl20x on sl20x.id = imss.slide_id
    left join check_slide on check_slide.cell_id = cell.cell_specimen_id
    left join na_objective on na_objective.ims_id = imgs63.image_series_id
    left join cortlayer on cortlayer.ims20_id = imgs20.image_series_id
    left join neuron_reconstructions nr on nr.specimen_id = cell.cell_specimen_id and nr.manual is true and nr.superseded is false
    left join well_known_files thumb on thumb.attachable_id = nr.id and thumb.well_known_file_type_id = 480715721 
    left join well_known_files hres_thumb on hres_thumb.attachable_id = nr.id and hres_thumb.well_known_file_type_id = 666522741 
    left join tissue_reviews tr on slice.id = tr.specimen_id 

    order by cell.cell_specimen_name
