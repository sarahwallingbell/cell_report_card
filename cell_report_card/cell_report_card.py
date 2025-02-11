import os
import pandas as pd
from datetime import datetime
from morph_utils.query import default_query_engine
from importlib.resources import files


def get_cell_report_card(q1_path, q2_path, query_engine=None):
    """
    Runs two LIMS Cell Report Card queries and combines output. 

    Parameters:
    q1_path (str): Path to the first SQL query file: "Cell Types cell report card"
    q2_path (str): Path to the second SQL query file: "cell report card without ephys or Tissue Processing"
    query_engine (callable, optional): LIMS query engine.

    Returns:
    pandas.DataFrame: Combined and cleaned cell report card data.
    """

    if query_engine is None: 
        query_engine = default_query_engine()

    # Cell Report Card Query 1: Cell Types cell report card
    with open(q1_path, "r", encoding="utf-8") as file:
        sql_query_1 = file.read().replace("\n", " ")

    # Cell Report Card Query 2: cell report card without ephys or Tissue Processing Template
    with open(q2_path, "r", encoding="utf-8") as file:
        sql_query_2 = file.read().replace("\n", " ")

    # Run queries 
    res_1 = query_engine(sql_query_1)
    q1_df = pd.DataFrame(res_1)

    res_2 = query_engine(sql_query_2)
    q2_df = pd.DataFrame(res_2)

    # Remove any cells in q2_df that are in q1_df 
    q2_df = q2_df[~q2_df['cell_specimen_id'].isin(q1_df['cell_specimen_id'])]

    # Split q1 df into the unique and duplicated specimen ids
    q1_df_unique = q1_df[~q1_df['cell_specimen_id'].duplicated(keep=False)]
    q1_df_duplicates = q1_df[q1_df['cell_specimen_id'].duplicated(keep=False)]

    # Remove duplicated specimen ids where the `morphologythumbnail` value between two duplicates does not match, and one of them is empty.
    # There are some other duplicates, but we can just leave them. (gave spec ids to RD, says they're all old) 
    remove_idxs = []
    for specimen_id in q1_df_duplicates.cell_specimen_id.unique(): 
        spid_duplicate_df = q1_df[q1_df['cell_specimen_id'] == specimen_id].fillna(0)
        non_unique_cols = spid_duplicate_df.columns[spid_duplicate_df.nunique() > 1].to_list()
        if 'morphologythumbnail' in non_unique_cols: 
            this_remove_idxs = spid_duplicate_df[non_unique_cols][spid_duplicate_df.morphologythumbnail == 0].index.tolist()
            remove_idxs.append(this_remove_idxs)

    remove_idxs = [item for sublist in remove_idxs for item in (sublist if isinstance(sublist, list) else [sublist])]
    q1_df_duplicates = q1_df_duplicates.drop(remove_idxs)

    # Stack the unique q1 cells, filtered duplicated q1 cells, and q2 cells
    cell_report_card_df = pd.concat([q1_df_unique, q1_df_duplicates, q2_df], ignore_index=True)

    # Set type for full df to string for saving to Access db
    cell_report_card_df = cell_report_card_df.fillna("")
    cell_report_card_df = cell_report_card_df.astype(str)

    return cell_report_card_df


def main():

    # Run Cell Report Card query 
    print('Running cell report card query...')
    q1_path = r"\\allen\programs\celltypes\workgroups\mousecelltypes\SarahWB\github_projects\cell_report_card\cell_report_card\cell_report_card_query_1.sql"
    q2_path = r"\\allen\programs\celltypes\workgroups\mousecelltypes\SarahWB\github_projects\cell_report_card\cell_report_card\cell_report_card_query_2.sql"
    cell_report_card = get_cell_report_card(q1_path, q2_path)

    # Save Locally 
    print('Saving results...')
    root_path = r'\\allen\programs\celltypes\workgroups\mousecelltypes\cell_report_card'
    cell_report_card.to_csv(os.path.join(root_path, 'cell_report_card.csv'), index=False)
    # cell_report_card.to_csv(os.path.join(root_path, 'cell_report_card.xlsx'), index=False)
    current_date = datetime.now().strftime('%Y%m%d')
    cell_report_card.to_csv(os.path.join(root_path, 'archive', f'cell_report_card_{current_date}.csv'), index=False)
    # cell_report_card.to_csv(os.path.join(root_path, 'archive', f'cell_report_card_{current_date}.xlsx'), index=False)

    print('Done!\n')


if __name__ == '__main__':
    main()