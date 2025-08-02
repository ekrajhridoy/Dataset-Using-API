from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import time

api = KaggleApi()
api.authenticate()

metadata = []
for page in range(1, 200):  # fetch 50 pages, adjust as needed
    datasets = api.dataset_list(sort_by='votes', page=page)

    for d in datasets:
        try:
            metadata.append({
                'name': d.title,
                'ref': d.ref,
                'url': f"https://www.kaggle.com/datasets/{d.ref}",
                'creator': d.owner_user.name if hasattr(d, 'owner_user') and d.owner_user else "Unknown",
                'downloads': d.download_count,
                'votes': d.vote_count,
                'size_MB': round(d.total_bytes / (1024 ** 2), 2) if hasattr(d,
                                                                            'total_bytes') and d.total_bytes else "N/A",
                'tags': ', '.join([tag.name for tag in d.tags]) if d.tags else '',
                'license': d.license_name,
                'kernel_count': d.kernel_count if hasattr(d, 'kernel_count') else 'N/A',
                'views': d.view_count if hasattr(d, 'view_count') else 'N/A',
                'usability_rating': d.usability_rating if hasattr(d, 'usability_rating') else 'N/A',
                'visibility': 'public' if not d.is_private else 'private'
            })
        except Exception as e:
            print(f"⚠️ Skipping dataset due to error: {e}")

    print(f"✅ Page {page} fetched")
    time.sleep(1)

# Save
df = pd.DataFrame(metadata)
df.to_csv("kaggle_dataset_metadata2.csv", index=False)
print(f"\n✅ Saved {len(df)} datasets to kaggle_dataset_metadata.csv")
