// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/br/pkg/storage"

	"github.com/pingcap/br/pkg/gluetidb"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/gluetikv"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/task"
	"github.com/pingcap/br/pkg/utils"
)

func runBackupCommand(command *cobra.Command, cmdName string) error {
	cfg := task.BackupConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}
	if cfg.IgnoreStats {
		// Do not run stat worker in BR.
		session.DisableStats4Test()
	}

	if cfg.Cron != "" {
		cr := cron.New(cron.WithSeconds())
		_, err := cr.AddFunc(cfg.Cron, func() {
			ctx := context.TODO()
			cfg = task.BackupConfig{Config: task.Config{LogProgress: HasLogFile()}}
			if err := cfg.ParseFromFlags(command.Flags()); err != nil {
				command.SilenceUsage = false
				panic(err)
			}
			if cfg.IgnoreStats {
				// Do not run stat worker in BR.
				session.DisableStats4Test()
			}
			u, err := storage.ParseRawURL(cfg.Storage)
			if err != nil {
				panic(err)
			}
			prefix := time.Now().Format("20060102150405")
			cfg.Storage = u.Scheme + "://" + u.Host + "/" + prefix
			fmt.Println("Storage path:", cfg.Storage)
			summary.InitCollector(HasLogFile())
			if err := task.RunBackup(ctx, gluetidb.New(), cmdName, &cfg); err != nil {
				log.Error("failed to backup", zap.Error(err))
				panic(err)
			}
		})
		if err != nil {
			log.Error("failed to set cron job", zap.Error(err))
			return errors.Trace(err)
		}
		fmt.Println("Cron job mode:", cfg.Cron)
		cr.Start()
		defer cr.Stop()
		for {
			time.Sleep(100 * time.Second)
		}
	}

	fmt.Println("Common mode:", cfg.Cron)
	if err := task.RunBackup(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to backup", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func runBackupRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseBackupConfigFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}
	if err := task.RunBackupRaw(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to backup raw kv", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "backup",
		Short:        "backup a TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			// Init logger, result summary, redact log and so on
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			utils.LogBRInfo()
			task.LogArguments(c)

			// Do not run ddl worker in BR.
			ddl.RunWorker = false

			summary.SetUnit(summary.BackupUnit)
			return nil
		},
	}
	command.AddCommand(
		newFullBackupCommand(),
		newTxnBackupCommand(),
		newDBBackupCommand(),
		newTableBackupCommand(),
		newRawBackupCommand(),
	)

	task.DefineBackupFlags(command.PersistentFlags())
	return command
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup all database",
		// prevents incorrect usage like `--checksum false` instead of `--checksum=false`.
		// the former, according to pflag parsing rules, means `--checksum=true false`.
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			// empty db/table means full backup.
			return runBackupCommand(command, task.CmdFullBackup)
		},
	}
	task.DefineFilterFlags(command)
	return command
}

func newTxnBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "txn",
		Short: "backup all txnkv",
		// prevents incorrect usage like `--checksum false` instead of `--checksum=false`.
		// the former, according to pflag parsing rules, means `--checksum=true false`.
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			// empty db/table means full backup.
			return runBackupCommand(command, task.CmdTxnBackup)
		},
	}
	task.DefineFilterFlags(command)
	return command
}

// newDBBackupCommand return a db backup subcommand.
func newDBBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "backup a database",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupCommand(command, task.CmdDBBackup)
		},
	}
	task.DefineDatabaseFlags(command)
	return command
}

// newTableBackupCommand return a table backup subcommand.
func newTableBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "backup a table",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupCommand(command, task.CmdTableBackup)
		},
	}
	task.DefineTableFlags(command)
	return command
}

// newRawBackupCommand return a raw kv range backup subcommand.
func newRawBackupCommand() *cobra.Command {
	// TODO: remove experimental tag if it's stable
	command := &cobra.Command{
		Use:   "raw",
		Short: "(experimental) backup a raw kv range from TiKV cluster",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupRawCommand(command, task.CmdRawBackup)
		},
	}

	task.DefineRawBackupFlags(command)
	return command
}
