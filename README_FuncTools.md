## Introducing Functional Tools within GTFS2GMNS

GTFS2GMNS is a Python package that serves as a class-based instance, specifically designed for reading, converting, analyzing, and visualizing GTFS data. The converted physical and service networks in GMNS format offer enhanced convenience for a variety of networkAg modeling tasks, including transit network routing, traffic flow assignment, simulation, and service network optimization.

### Input for class GTFS2GMNS

- **gtfs_input_dir** :  str, the dir store GTFS data. **GTFS2GMNS is capable of reading multiple GTFS data sets.**
- **time_period**: str, the time period sprcified (for data selection), default is "07:00:00_08:00:00"
- **date_period**: list, user can specified exact data or dates for selection
- **gtfs_output_dir**: str, the output folder to save data. defalut is ""
- **isSaveToCSV**: bool, whether to save gmns node and link to local machine, default is True

*Code Example*

```python
from gtfs2gmns import GTFS2GMNS

if __name__ == "__main__":
    gtfs_input_dir = r"Your-Path-Folder-To-GTFS-Data"

    # Explain: GMNS2GMNS is capable of reading multiple GTFS data sets
    """
	--root folder
	    -- subfolder (GTFS data of agency 1)
	    -- subfolder (GTFS data of agency 2)
	    -- subfolder (GTFS data of agency 3)
	    -- ...
	then, assign gtfs_input_foler = root folder
    """

    time_period = "00:00:00_23:59:59"
    date_period = []

    gg = GTFS2GMNS(gtfs_input_dir, time_period, date_period, gtfs_output_dir="", isSaveToCSV=False)

```

### Functions and Attributes

| func_type     | func_name                    | Python exmple   | Input | Output    | Remark                                                        |
| :------------ | :--------------------------- | :-------------- | ----- | --------- | ------------------------------------------------------------- |
| read-show     | agency                       | `gg.agency`   | NA    | Dataframe | This attribute load and return agency data from source folder |
|               | calendar                     | `gg.calendar` |       |           |                                                               |
|               | calendar_dates               |                 |       |           |                                                               |
|               | fare_attributes              |                 |       |           |                                                               |
|               | fare_rules                   |                 |       |           |                                                               |
|               | feed_info                    |                 |       |           |                                                               |
|               | frequencies                  |                 |       |           |                                                               |
|               | routes                       |                 |       |           |                                                               |
|               | shapes                       |                 |       |           |                                                               |
|               | stops                        |                 |       |           |                                                               |
|               | stop_times                   |                 |       |           |                                                               |
|               | trips                        |                 |       |           |                                                               |
|               | transfers                    |                 |       |           |                                                               |
|               | timepoints                   |                 |       |           |                                                               |
|               | timepoint_times              |                 |       |           |                                                               |
|               | trip_routes                  |                 |       |           |                                                               |
|               | stops_freq                   |                 |       |           |                                                               |
|               | routes_freq                  |                 |       |           |                                                               |
|               | rute_segments                |                 |       |           |                                                               |
|               | route_segment_speed          |                 |       |           |                                                               |
|               | vis_stops_freq               |                 |       |           |                                                               |
| analysis      | vis_routes_fres              |                 |       |           |                                                               |
|               | vis_route_segment_speed      |                 |       |           |                                                               |
|               | vis_route_segment_runtime    |                 |       |           |                                                               |
|               | vis_route_stop_speed_heatmap |                 |       |           |                                                               |
|               | vis_spacetime_trajectory     |                 |       |           |                                                               |
|               | equity_alanysis              |                 |       |           |                                                               |
|               | accessibility_analysis       |                 |       |           |                                                               |
|               | load_gtfs                    |                 |       |           |                                                               |
|               | gen_gmns_node_link           |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
| visualization |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |
|               |                              |                 |       |           |                                                               |

a
