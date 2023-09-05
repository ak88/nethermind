// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain.Synchronization;
using Nethermind.Db;
using Nethermind.Synchronization.FastBlocks;
using Nethermind.Synchronization.ParallelSync;
using Nethermind.Synchronization.SnapSync;

namespace Nethermind.Synchronization.DbTuner;

public class SyncDbTuner
{
    private readonly IDb _stateDb;
    private readonly IDb _codeDb;
    private readonly IDb _blockDb;
    private readonly IDb _receiptBlocksDb;
    private readonly IDb _receiptTxIndexDb;

    private ITunableDb.TuneType _tuneType;
    private ITunableDb.TuneType _blocksDbTuneType;
    private ITunableDb.TuneType _receiptsBlocksDbTuneType;

    public SyncDbTuner(
        ISyncConfig syncConfig,
        ISyncFeed<SnapSyncBatch>? snapSyncFeed,
        ISyncFeed<BodiesSyncBatch>? bodiesSyncFeed,
        ISyncFeed<ReceiptsSyncBatch>? receiptSyncFeed,
        IDb stateDb,
        IDb codeDb,
        IDb blockDb,
        IDb receiptBlocksDb,
        IDb receiptTxIndexDb
    )
    {
        // Only these three make sense as they are write heavy
        // Headers is used everywhere, so slowing read might slow the whole sync.
        // Statesync is read heavy, Forward sync is just plain too slow to saturate IO.
        if (snapSyncFeed != null)
        {
            snapSyncFeed.StateChanged += SnapStateChanged;
        }

        if (bodiesSyncFeed != null)
        {
            bodiesSyncFeed.StateChanged += BodiesStateChanged;
        }

        if (receiptSyncFeed != null)
        {
            receiptSyncFeed.StateChanged += ReceiptsStateChanged;
        }

        _stateDb = stateDb;
        _codeDb = codeDb;
        _blockDb = blockDb;
        _receiptBlocksDb = receiptBlocksDb;
        _receiptTxIndexDb = receiptTxIndexDb;

        _tuneType = syncConfig.TuneDbMode;
        _blocksDbTuneType = syncConfig.BlocksDbTuneDbMode;
        _receiptsBlocksDbTuneType = syncConfig.ReceiptsDbTuneDbMode;
    }

    private void SnapStateChanged(object? sender, SyncFeedStateEventArgs e)
    {
        if (e.NewState == SyncFeedState.Active)
        {
            if (_stateDb is ITunableDb stateDb)
            {
                stateDb.Tune(_tuneType);
            }
            if (_codeDb is ITunableDb codeDb)
            {
                codeDb.Tune(_tuneType);
            }
        }
        else if (e.NewState == SyncFeedState.Finished)
        {
            if (_stateDb is ITunableDb stateDb)
            {
                stateDb.Tune(ITunableDb.TuneType.Default);
            }
            if (_codeDb is ITunableDb codeDb)
            {
                codeDb.Tune(ITunableDb.TuneType.Default);
            }
        }
    }

    private void BodiesStateChanged(object? sender, SyncFeedStateEventArgs e)
    {
        if (e.NewState == SyncFeedState.Active)
        {
            if (_blockDb is ITunableDb blockDb)
            {
                blockDb.Tune(_blocksDbTuneType);
            }
        }
        else if (e.NewState == SyncFeedState.Finished)
        {
            if (_blockDb is ITunableDb blockDb)
            {
                blockDb.Tune(ITunableDb.TuneType.Default);
            }
        }
    }

    private void ReceiptsStateChanged(object? sender, SyncFeedStateEventArgs e)
    {
        if (e.NewState == SyncFeedState.Active)
        {
            // ReceiptBlocks column can enable blob files.
            // But tx index should definitely not.
            if (_receiptBlocksDb is ITunableDb receiptBlocksDb)
            {
                receiptBlocksDb.Tune(_receiptsBlocksDbTuneType);
            }
            if (_receiptTxIndexDb is ITunableDb receiptTxIndexDb)
            {
                receiptTxIndexDb.Tune(_tuneType);
            }
        }
        else if (e.NewState == SyncFeedState.Finished)
        {
            if (_receiptBlocksDb is ITunableDb receiptBlocksDb)
            {
                receiptBlocksDb.Tune(ITunableDb.TuneType.Default);
            }
            if (_receiptTxIndexDb is ITunableDb receiptTxIndexDb)
            {
                receiptTxIndexDb.Tune(ITunableDb.TuneType.Default);
            }
        }
    }
}
