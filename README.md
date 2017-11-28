# GOAT - Geographic Objects for Augmenting Topography
Thin-client for interacting with photon (based on OpenStreetMap) java server running on port 2322 localhost or elsewhere. It provides a way to geocode and reverse geocode locations/coordinates.

At the bottom of `goat/forward.py` and `goat/reverse.py` are included specific examples on how to achieve geocoding or reverse geocoding on localhost.

Simply execute the following command at top level, with the photon server running:

```bash
python3 goat/forward.py
```
Please note that the reverse geocoder takes a significantly longer time to execute. Significant improvements are ongoing, but this is inherently a harder problem than the forward geocoder.

To setup the photon server locally, see instructions at https://github.com/komoot/photon. Note that this is a very memory and disk intensive server. 
